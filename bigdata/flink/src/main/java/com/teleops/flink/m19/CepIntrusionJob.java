package com.teleops.flink.m19;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * 文件名: bigdata/flink/src/main/java/com/teleops/flink/m19/CepIntrusionJob.java
 * 所属模块: 模块 19 —— Flink CEP 攻击模式检测（场景二的实时检测核心）
 * 运行方式（依赖 telecom-netflow Topic 有数据，见 lab-19 / 场景二）:
 *   docker exec flink-jobmanager /opt/flink/bin/flink run \
 *     -c com.teleops.flink.m19.CepIntrusionJob \
 *     /opt/flink/usrlib/teleops-flink-jobs.jar
 * 教学目标（对应模块 19 文档 CEP 节）:
 *   检测"SYN Flood 攻击模式"：60 秒内同一源 IP 出现 ≥ 5 次 SYN 包记录。
 *   Pattern 三要素演示：
 *     timesOrMore(5)  —— 量词（至少 5 次）
 *     within(Time.seconds(60)) —— 时间约束（防"全天凑数"的误报）
 *     SimpleCondition —— 条件（仅 SYN 标记的事件参与匹配）
 */
public class CepIntrusionJob {

    // NetFlow 记录格式（场景二生成器）：{"src_ip":"1.2.3.4","flags":"S","dst_port":80,"ts":...}
    private static final java.util.regex.Pattern P_SRC = java.util.regex.Pattern.compile("\"src_ip\"\\s*:\\s*\"([^\"]+)\"");
    private static final java.util.regex.Pattern P_FLAGS = java.util.regex.Pattern.compile("\"flags\"\\s*:\\s*\"([^\"]+)\"");
    private static final java.util.regex.Pattern P_TS = java.util.regex.Pattern.compile("\"ts\"\\s*:\\s*(\\d+)");

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.enableCheckpointing(10000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka-1:9092,kafka-2:9092,kafka-3:9092")
                .setTopics("telecom-netflow")
                .setGroupId("flink-cep-ids")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 事件时间：NetFlow 记录自带采集时间戳 ts
        DataStream<FlowEvent> flows = env.fromSource(
                        source,
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withIdleness(Duration.ofSeconds(30))   // 空闲分区防冻结：无数据分区超过 30s 标记 idle，不拖累全局 watermark（生产必配！）
                                .withTimestampAssigner((line, ts) -> {
                                    Matcher m = P_TS.matcher(line);
                                    return m.find() ? Long.parseLong(m.group(1))
                                                    : System.currentTimeMillis();
                                }),
                        "kafka-netflow")
                .map(CepIntrusionJob::parse);

        // ---- CEP 模式：60 秒内同一 src_ip 的 ≥5 次 SYN ----
        Pattern<FlowEvent, ?> synFlood = Pattern.<FlowEvent>begin("syn")
                .where(new SimpleCondition<FlowEvent>() {
                    @Override
                    public boolean filter(FlowEvent e) {
                        // TCP flags 含 S 且不含 A：纯 SYN（握手第一步）
                        return "S".equals(e.flags) && !e.flags.contains("A");
                    }
                })
                .timesOrMore(5)                 // ≥ 5 次
                .consecutive()                  // 允许中间夹其他事件也可累计（松散连续）
                .within(Time.seconds(60));      // 时间窗约束

        // 按 src_ip 分组匹配：CEP 的 keyBy 语义——模式在每组内独立运行
        PatternStream<FlowEvent> patternStream = CEP.pattern(
                flows.keyBy(e -> e.srcIp), synFlood);

        // 匹配结果 → 攻击告警事件（场景二 Agent 的输入）
        DataStream<String> alerts = patternStream.select(
                new PatternSelectFunction<FlowEvent, String>() {
                    @Override
                    public String select(Map<String, List<FlowEvent>> match) {
                        List<FlowEvent> syns = match.get("syn");
                        FlowEvent first = syns.get(0);
                        return String.format(
                                "{\"type\":\"ddos_alert\",\"attack_type\":\"SYN_Flood\"," +
                                "\"src_ip\":\"%s\",\"syn_count\":%d,\"first_seen\":%d," +
                                "\"severity\":\"critical\"}",
                                first.srcIp, syns.size(), first.ts);
                    }
                });

        alerts.print("DDoS告警");

        // 告警写回 Kafka telecom-alerts（与 KPI 告警同一下游，Agent 消费）
        alerts.sinkTo(org.apache.flink.connector.kafka.sink.KafkaSink.<String>builder()
                .setBootstrapServers("kafka-1:9092,kafka-2:9092,kafka-3:9092")
                .setRecordSerializer(org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema.builder()
                        .setTopic("telecom-alerts")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                // 默认 AT_LEAST_ONCE；EXACTLY_ONCE 需开启 Kafka 事务（文档 4.3）
                .build()).name("ddos-alert-sink");

        env.execute("M19-CepIntrusionJob");
    }

    private static FlowEvent parse(String line) {
        java.util.regex.Matcher ms = P_SRC.matcher(line);
        java.util.regex.Matcher mf = P_FLAGS.matcher(line);
        java.util.regex.Matcher mt = P_TS.matcher(line);
        FlowEvent e = new FlowEvent();
        e.srcIp = ms.find() ? ms.group(1) : "unknown";
        e.flags = mf.find() ? mf.group(1) : "";
        e.ts = mt.find() ? Long.parseLong(mt.group(1)) : System.currentTimeMillis();
        return e;
    }

    /** NetFlow 事件载体（CEP 要求字段可访问，POJO 即可）。 */
    public static class FlowEvent {
        public String srcIp;
        public String flags;
        public long ts;

        public FlowEvent() {}
    }
}
