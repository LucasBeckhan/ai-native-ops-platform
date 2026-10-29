package com.teleops.flink.m19;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * 文件名: bigdata/flink/src/main/java/com/teleops/flink/m19/KafkaKpiAlertJob.java
 * 所属模块: 模块 19 / 场景一 —— 分切片 SLA 的实时 KPI 告警作业
 * 运行方式:
 *   docker exec flink-jobmanager /opt/flink/bin/flink run -d \
 *     -c com.teleops.flink.m19.KafkaKpiAlertJob /opt/flink/usrlib/teleops-flink-jobs.jar
 * 教学目标:
 *   1) Kafka→Flink→Kafka+ES 的完整实时告警链路（场景一的数据面）；
 *   2) **分切片 SLA 阈值**：eMBB 看吞吐、uRLLC 看时延、mMTC 看连接——
 *      固定阈值会把天然低吞吐的 mMTC 全部误报（本作业第一版的真实教训）；
 *   3) 复合 keyBy（sliceId|deviceId）与增量聚合+窗口元数据的标准组合。
 */
public class KafkaKpiAlertJob {

    // ---- 分切片 SLA 阈值（生产应下发自配置中心/网管北向接口）----
    // throughputFloor: 30s 窗口平均吞吐低于此值告警（Mbps）；latencyCeiling: 平均时延高于此值告警（ms）
    private static final Map<String, double[]> SLA = new HashMap<>();
    static {
        //                     {throughputFloor, latencyCeiling}
        SLA.put("slice-eMBB-001",  new double[]{700.0, 50.0});
        SLA.put("slice-uRLLC-001", new double[]{50.0, 10.0});
        SLA.put("slice-mMTC-001",  new double[]{2.0, 100.0});
    }

    // ---- 正则解析（瘦 jar 不携带 JSON 库，见模块 19 文档 2 节说明）----
    private static final java.util.regex.Pattern P_DEV =
            java.util.regex.Pattern.compile("\"deviceId\"\\s*:\\s*\"([^\"]+)\"");
    private static final java.util.regex.Pattern P_TP =
            java.util.regex.Pattern.compile("\"throughput\"\\s*:\\s*([0-9.]+)");
    private static final java.util.regex.Pattern P_LAT =
            java.util.regex.Pattern.compile("\"latency_ms\"\\s*:\\s*([0-9.]+)");
    private static final java.util.regex.Pattern P_TS =
            java.util.regex.Pattern.compile("\"ts\"\\s*:\\s*(\\d+)");
    private static final java.util.regex.Pattern P_SLICE =
            java.util.regex.Pattern.compile("\"sliceId\"\\s*:\\s*\"([^\"]+)\"");

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10000);

        // ---- Source：容器网络用服务名（宿主机客户端才用 localhost:39092）----
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-1:9092,kafka-2:9092,kafka-3:9092")
                .setTopics("telecom-gnb-kpi")
                .setGroupId("flink-kpi-alert")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // withIdleness：空闲分区防 watermark 冻结（lab-19 实测教训，生产必配）
        DataStream<Kpi> events = env.fromSource(
                        source,
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withIdleness(Duration.ofSeconds(30))
                                .withTimestampAssigner((line, ts) -> extractEventTime(line)),
                        "kafka-gnb-kpi")
                .filter(KafkaKpiAlertJob::isValid)
                .map(KafkaKpiAlertJob::parse);

        // ---- 聚合：30s 事件时间滚动窗口，同时算平均吞吐与时延 ----
        DataStream<String> alerts = events
                // 复合键：切片+设备（同切片 KPI 保序，同设备不同切片分开判定）
                .keyBy(k -> k.sliceId + "|" + k.deviceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .aggregate(
                        // 增量聚合：状态只有 [tpSum, latSum, count]，与窗口数据量无关
                        new org.apache.flink.api.common.functions.AggregateFunction<
                                Kpi, double[], double[]>() {
                            @Override
                            public double[] createAccumulator() { return new double[3]; }
                            @Override
                            public double[] add(Kpi k, double[] acc) {
                                acc[0] += k.throughput;
                                acc[1] += k.latency;
                                acc[2] += 1;
                                return acc;
                            }
                            @Override
                            public double[] getResult(double[] acc) {
                                return new double[]{acc[0] / acc[2], acc[1] / acc[2]};
                            }
                            @Override
                            public double[] merge(double[] a, double[] b) {
                                return new double[]{a[0] + b[0], a[1] + b[1], a[2] + b[2]};
                            }
                        },
                        // SLA 判定 + 告警组装（窗口元数据进告警的时间区间字段）
                        new ProcessWindowFunction<double[], String, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context ctx,
                                                Iterable<double[]> avg, Collector<String> out) {
                                double[] v = avg.iterator().next();
                                TimeWindow w = ctx.window();
                                String[] parts = key.split("\\|");
                                String sliceId = parts[0], deviceId = parts[1];
                                double[] sla = SLA.getOrDefault(
                                        sliceId, new double[]{0.0, Double.MAX_VALUE});
                                if (v[0] < sla[0] || v[1] > sla[1]) {
                                    // 违规原因细化：吞吐不达标 / 时延超标
                                    String reason = v[0] < sla[0]
                                            ? String.format("吞吐 %.1f<%.0fMbps", v[0], sla[0])
                                            : String.format("时延 %.1f>%.0fms", v[1], sla[1]);
                                    out.collect(String.format(
                                            "{\"type\":\"kpi_alert\",\"device_id\":\"%s\"," +
                                            "\"slice_id\":\"%s\",\"window_start\":%d,\"window_end\":%d," +
                                            "\"avg_throughput\":%.1f,\"avg_latency_ms\":%.1f," +
                                            "\"reason\":\"%s\",\"severity\":\"critical\"," +
                                            "\"message\":\"KPI 劣化\"}",
                                            deviceId, sliceId, w.getStart(), w.getEnd(),
                                            v[0], v[1], reason));
                                }
                            }
                        });

        alerts.print("KPI告警");

        // Sink 1：Kafka（Agent 实时消费）
        alerts.sinkTo(KafkaSink.<String>builder()
                .setBootstrapServers("kafka-1:9092,kafka-2:9092,kafka-3:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("telecom-alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build()).name("kafka-alerts-sink");

        // Sink 2：ES（slice-sla Skill 的 es_query_alerts 检索面）
        alerts.addSink(new com.teleops.flink.common.EsBulkSink(
                "http://elasticsearch:9200/_bulk")).name("es-alert-sink");

        env.execute("M19-KafkaKpiAlertJob");
    }

    // ---- 解析辅助 ----
    private static boolean isValid(String line) {
        return line != null && line.contains("deviceId") && line.contains("throughput");
    }

    private static Kpi parse(String line) {
        java.util.regex.Matcher md = P_DEV.matcher(line);
        java.util.regex.Matcher mt = P_TP.matcher(line);
        java.util.regex.Matcher ml = P_LAT.matcher(line);
        if (md.find() && mt.find()) {
            java.util.regex.Matcher msl = P_SLICE.matcher(line);
            double lat = ml.find() ? Double.parseDouble(ml.group(1)) : 0.0;
            return new Kpi(md.group(1), msl.find() ? msl.group(1) : "unknown",
                           Double.parseDouble(mt.group(1)), lat);
        }
        // 脏数据兜底：永不告警的占位（生产应进 sideOutput 死信队列）
        return new Kpi("dirty", "unknown", Double.MAX_VALUE, 0.0);
    }

    private static long extractEventTime(String line) {
        java.util.regex.Matcher mts = P_TS.matcher(line);
        if (mts.find()) {
            return Long.parseLong(mts.group(1));
        }
        return System.currentTimeMillis();
    }

    /** KPI 载体。 */
    public static class Kpi {
        public String deviceId;
        public String sliceId;
        public double throughput;
        public double latency;

        public Kpi() {}
        public Kpi(String deviceId, String sliceId, double throughput, double latency) {
            this.deviceId = deviceId;
            this.sliceId = sliceId;
            this.throughput = throughput;
            this.latency = latency;
        }
    }
}
