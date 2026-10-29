package com.teleops.flink.m18;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;

/**
 * 文件名: bigdata/flink/src/main/java/com/teleops/flink/m18/EventTimeWindowJob.java
 * 所属模块: 模块 18 —— 事件时间、Watermark 与乱序处理
 * 运行方式:
 *   docker exec flink-jobmanager /opt/flink/bin/flink run \
 *     /opt/flink/usrlib/teleops-flink-jobs.jar \
 *     -c com.teleops.flink.m18.EventTimeWindowJob
 * 教学目标（对应模块 18 文档 2/3 节）:
 *   1) Event Time 与 Processing Time 的区别：用"乱序到达"的数据流演示——
 *      事件时间戳是数据自带的，与到达顺序无关；
 *   2) Watermark 的本质：一个"时间进度声明"——单调递增的 watermark 之前的
 *      事件被认为到齐，窗口据此触发；
 *   3) 迟到数据：超过 watermark 的事件默认被丢弃（本作业打印），生产用
 *      sideOutputLateData 旁路输出（见文档 3.3）。
 */
public class EventTimeWindowJob {

    /** 5G 基站 KPI 事件：时间戳取事件发生时刻（而非处理时刻）。 */
    public static class KpiEvent {
        public long eventTime;   // 事件时间（epoch ms）
        public String deviceId;  // 基站ID
        public double throughput;

        public KpiEvent() {}
        public KpiEvent(long eventTime, String deviceId, double throughput) {
            this.eventTime = eventTime;
            this.deviceId = deviceId;
            this.throughput = throughput;
        }
        @Override
        public String toString() {
            return "KpiEvent{t=" + eventTime + ", dev=" + deviceId + ", tp=" + throughput + "}";
        }
    }

    /**
     * 模拟乱序数据源：按 100ms 的"事件时间"步进生成事件，
     * 但随机有 0~300ms 的乱序延迟（事件时间倒退）。
     */
    public static class OutOfOrderSource implements SourceFunction<KpiEvent> {
        private volatile boolean running = true;
        private final int count;

        public OutOfOrderSource(int count) { this.count = count; }

        @Override
        public void run(SourceContext<KpiEvent> ctx) throws Exception {
            Random rnd = new Random(42);   // 固定种子：实验可复现
            long base = System.currentTimeMillis() - count * 100L;
            for (int i = 0; i < count && running; i++) {
                long eventTime = base + i * 100L;
                // 每 5 条注入一次 0~300ms 的乱序（事件时间提前已出现过）
                long disorder = (i % 5 == 0) ? rnd.nextInt(300) : 0;
                ctx.collectWithTimestamp(
                        new KpiEvent(Math.max(base, eventTime - disorder), "eNB-1001", 800 + rnd.nextInt(50)),
                        Math.max(base, eventTime - disorder));
                Thread.sleep(10);   // 加速演示：真实流不会这么快
            }
        }

        @Override
        public void cancel() { running = false; }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度 1：保证控制台输出顺序可读（生产按吞吐设置，见模块 20）
        env.setParallelism(1);
        // Checkpoint（模块 18 第 5 节）：10 秒一次，演示状态容错
        env.enableCheckpointing(10000);

        DataStream<KpiEvent> events = env
                .addSource(new OutOfOrderSource(200))
                .name("乱序KPI数据源");

        // Watermark 策略：允许 200ms 乱序。含义——watermark = 已见最大事件时间 - 200ms，
        // 落后于 watermark 的事件即"迟到"。
        WatermarkStrategy<KpiEvent> strategy = WatermarkStrategy
                .<KpiEvent>forBoundedOutOfOrderness(Duration.ofMillis(200))
                .withTimestampAssigner((event, ts) -> event.eventTime);

        // 迟到数据的完整处理（旁路输出）见模块 18 文档 3.3；
        // 本作业聚焦窗口触发与 watermark 传播
        DataStream<KpiEvent> withWatermarks = events
                .assignTimestampsAndWatermarks(strategy);

        // 事件时间滚动窗口：1 秒一个窗口，按基站聚合平均吞吐
        DataStream<String> result = withWatermarks
                .keyBy(e -> e.deviceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                // 标准组合：AggregateFunction（增量聚合，状态小）+
                // ProcessWindowFunction（拿窗口元数据）。面试常考为什么这样组合：
                // 纯 ProcessWindowFunction 会把窗口内全部数据攒在状态里，内存差。
                .aggregate(new AggregateFunction<KpiEvent, double[], Double>() {
                    @Override
                    public double[] createAccumulator() { return new double[]{0, 0}; }

                    @Override
                    public double[] add(KpiEvent e, double[] acc) {
                        acc[0] += e.throughput;   // 总和
                        acc[1] += 1;              // 条数
                        return acc;
                    }

                    @Override
                    public Double getResult(double[] acc) {
                        return acc[0] / acc[1];   // 平均吞吐
                    }

                    @Override
                    public double[] merge(double[] a, double[] b) {
                        return new double[]{a[0] + b[0], a[1] + b[1]};
                    }
                }, new org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<Double, String, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {
                    @Override
                    public void process(String key, Context ctx,
                                        Iterable<Double> elements, Collector<String> out) {
                        org.apache.flink.streaming.api.windowing.windows.TimeWindow w = ctx.window();
                        out.collect("设备=" + key + " 窗口=[" + w.getStart() + "," + w.getEnd()
                                + ") avg_throughput=" + elements.iterator().next());
                    }
                });

        result.print("1s窗口聚合");

        env.execute("M18-EventTimeWindowJob");
    }
}
