package com.teleops.flink.m20;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 文件名: bigdata/flink/src/main/java/com/teleops/flink/m20/EsAlertPipelineJob.java
 * 所属模块: 模块 20 —— 完整实时监控管道（Kafka → Flink CEP → Elasticsearch）
 * 运行方式:
 *   docker exec flink-jobmanager /opt/flink/bin/flink run \
 *     -c com.teleops.flink.m20.EsAlertPipelineJob \
 *     /opt/flink/usrlib/teleops-flink-jobs.jar
 * 说明:
 *   - 这是模块 20 "完整监控管道" 的作业实现：Kafka 源 → CEP 检测设备闪断
 *     模式（1 分钟内 link_down → link_up ≥ 3 次的抖动）→ ES 落库。
 *   - ES 写入用 java.net.http.HttpClient 直调 REST（_bulk API）——教学实现，
 *     不引入 flink-connector-elasticsearch 依赖；生产建议官方连接器
 *     （批量/重试/flush 语义更完善，见模块 20 文档 4.2 的对比）。
 *   - 内置注入源 FaultSource：不依赖外部数据也能演示管道（两个数据路径可选）。
 */
public class EsAlertPipelineJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10000);

        DataStream<String> source;
        if (args.length > 0 && "kafka".equals(args[0])) {
            // 数据路径 A：从 Kafka telecom-gnb-alarms 消费真实告警流
            source = env.fromSource(
                    KafkaSource.<String>builder()
                            .setBootstrapServers("kafka-1:9092,kafka-2:9092,kafka-3:9092")
                            .setTopics("telecom-gnb-alarms")
                            .setGroupId("flink-es-pipeline")
                            .setStartingOffsets(OffsetsInitializer.latest())
                            .setValueOnlyDeserializer(new SimpleStringSchema())
                            .build(),
                    // withIdleness：空闲分区 30s 后标记 idle，不拖累全局 watermark（生产必配）
                    WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withIdleness(Duration.ofSeconds(30)),
                    "kafka-alarms");
        } else {
            // 数据路径 B（默认）：内置注入源，自包含演示
            // 同样要分配 watermark：SourceFunction 不会自动产生时间语义
            source = env.addSource(new FlapSource()).name("flap-injector")
                    .assignTimestampsAndWatermarks(
                            WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                    .withTimestampAssigner((line, ts) -> {
                                        java.util.regex.Matcher m = java.util.regex.Pattern
                                                .compile("\"ts\":(\\d+)").matcher(line);
                                        return m.find() ? Long.parseLong(m.group(1))
                                                        : System.currentTimeMillis();
                                    }));
        }

        // CEP：60 秒内 link_down ≥ 3 次 = 链路抖动
        Pattern<String, ?> flap = Pattern.<String>begin("down")
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String line) {
                        return line.contains("\"link_down\"");
                    }
                })
                .timesOrMore(3)
                .within(Time.seconds(60));

        PatternStream<String> patternStream = CEP.pattern(
                source.keyBy(EsAlertPipelineJob::deviceOf), flap);

        DataStream<String> flapAlerts = patternStream.select(
                new PatternSelectFunction<String, String>() {
                    @Override
                    public String select(Map<String, List<String>> match) {
                        List<String> downs = match.get("down");
                        return String.format(
                                "{\"@timestamp\":%d,\"type\":\"link_flap\",\"device_id\":\"%s\"," +
                                "\"flap_count\":%d,\"severity\":\"major\",\"message\":\"链路 %d 次闪断\"}",
                                System.currentTimeMillis(), deviceOf(downs.get(0)), downs.size(), downs.size());
                    }
                });

        flapAlerts.print("链路抖动告警");

        // Sink 1：告警写入 Kafka（供 Agent 消费）
        flapAlerts.sinkTo(KafkaSink.<String>builder()
                .setBootstrapServers("kafka-1:9092,kafka-2:9092,kafka-3:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("telecom-alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                // 默认 AT_LEAST_ONCE；EXACTLY_ONCE 需开启 Kafka 事务（文档 4.3）
                .build()).name("kafka-alert-sink");

        // Sink 2：告警写入 Elasticsearch（REST _bulk 直写，教学实现）
        flapAlerts.addSink(new com.teleops.flink.common.EsBulkSink(
                "http://elasticsearch:9200/_bulk")).name("es-bulk-sink");

        env.execute("M20-EsAlertPipelineJob");
    }

    private static String deviceOf(String line) {
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("\"device_id\"\\s*:\\s*\"([^\"]+)\"").matcher(line);
        return m.find() ? m.group(1) : "unknown";
    }

    /** 内置注入源：每 2 秒产生一次 link_down/link_up 抖动序列。 */
    public static class FlapSource implements SourceFunction<String> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            Random rnd = new Random(11);
            long i = 0;
            while (running) {
                String device = "eNB-10" + (10 + rnd.nextInt(5));
                // 一组抖动：3 次 down/up（间隔 300ms），然后安静 5 秒
                for (int j = 0; j < 3 && running; j++) {
                    ctx.collect("{\"ts\":" + System.currentTimeMillis() + ",\"device_id\":\"" + device
                            + "\",\"link_down\":true,\"seq\":" + (i++) + "}");
                    Thread.sleep(300);
                    ctx.collect("{\"ts\":" + System.currentTimeMillis() + ",\"device_id\":\"" + device
                            + "\",\"link_up\":true,\"seq\":" + (i++) + "}");
                    Thread.sleep(300);
                }
                Thread.sleep(5000);
            }
        }

        @Override
        public void cancel() { running = false; }
    }


}
