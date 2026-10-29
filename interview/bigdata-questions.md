# 大数据方向面试题汇总（Kafka / Flink / Spark）

> 完整解析分布：Kafka 23 道（模块 15-17 各文件）、Flink 9 道、Spark 10 道。本文件索引 + 速答补充。

## 一、Kafka（23 道 + 速答 8 道）

**完整解析**：`bigdata/kafka/docs/interview-m15.md`（8 道：高吞吐原理/消息不丢失三端/ISR 与 HW/LEO/KRaft/顺序性/分区规划/采集管道设计/页缓存陷阱）、`interview-m16.md`（8 道：幂等原理/事务 EOS/再平衡风暴/位移提交/两类分配/Streams 选型/攒批权衡/多 Topic 订阅）、`interview-m17.md`（7 道：Lag 排查/URP 定位/容量规划/Topic 治理/滚动升级/MM2 容灾/监控大盘）。

**速答补充**：
- **Q 零拷贝为什么在 TLS 下失效？** sendfile 数据不经用户态；TLS 必须加解密→数据进用户态→退化为普通路径。
- **Q KRaft 的 Controller 仲裁怎么选主？** Raft 多数派选举，任期制防脑裂；Broker 作为元数据日志的"消费者"拉取重放。
- **Q 事务消息的 LSO 是什么？** Last Stable Offset——read_committed 消费者只能读到 LSO；未决事务会卡住 LSO（长事务拖垮下游的红线）。
- **Q 消息积压的紧急处理？** 先判形态（模块 17）：倾斜→加盐；均匀慢→扩消费者+优化；可丢→按时间跳过+事后重算；扩容兜底→加分区（注意乱序窗口）。
- **Q 幂等为什么限制 in-flight=5？** Broker 的序列号滑动窗口去重边界为 5 批。
- **Q min.isr=1 会怎样？** acks=all 退化成 acks=1（伪安全）——单机故障且回退风险。
- **Q 什么场景 RF=2 可接受？** 可重建的日志/指标明细 + 有下游副本；网管告警类必须 RF=3。
- **Q MirrorMaker2 的 RPO 是多少？** 异步复制，RPO=复制延迟（秒级）；强一致需双写（代价大）。

## 二、Flink（9 道 + 速答 7 道）

**完整解析**：`bigdata/flink/docs/interview-m18-m20.md`（时间语义与 Watermark/Checkpoint 流程与 Spark 对比/反压定位/CEP 原理与边界/Exactly-Once 语义/倾斜治理/State Backend/大作业设计/Checkpoint 失败排查）。

**速答补充**：
- **Q withIdleness 什么时候必须配？** 多分区 Source 且数据不均（本项目实测：空闲分区冻结全局 watermark，CEP 永不输出）。
- **Q timesOrMore 什么时候输出匹配？** watermark 推过 within 窗口末端（开放量词需确认"不再有更多"）——lab-19 实证。
- **Q unaligned checkpoint 的代价？** 状态里多存 in-flight 数据，快照变大；换取反压时 Checkpoint 成功。
- **Q Flink 的 Slot 是什么？和 Task 什么关系？** TM 内资源分片；slot sharing 让同一作业的不同算子链共存一个 slot（并行度预算的单位）。
- **Q Savepoint 与 Checkpoint 区别？** 手动/格式稳定/用于升级迁移 vs 自动/容错；Savepoint 停机升级的锚点。
- **Q Flink 反压和 Spark 反压的区别？** Flink 信用制（credit-based）天然流控到 Source；Spark 微批天然限速（批次间隔即节流阀）。
- **Q 两条流怎么 join？** 窗口 join（时间对齐）/interval join（区间匹配）/connect（自定义状态——双流合并的万能钥匙）。

## 三、Spark（10 道 + 速答 6 道）

**完整解析**：`bigdata/spark/docs/interview-m21-m24.md`（宽窄依赖与 Stage/Shuffle/倾斜五解法/Catalyst/内存 GC/cache/Spark on K8s/EOS/微批选型/亿级关联分析设计）。

**速答补充**：
- **Q AQE 的三个能力？** 小分区合并、倾斜 Join 自动拆分、Join 策略动态切换（3.x 必开）。
- **Q spark.shuffle.partitions=200 默认对小作业意味着什么？** 小文件+调度开销；AQE 合并是解法，静态作业手动调小。
- **Q cache 与 checkpoint 的区别？** 缓存（血统保留，节点丢失重算）vs 物理副本（血统截断）；checkpoint 用于长血统/迭代计算。
- **Q PySpark 为什么比 Scala 慢？** Python 进程与 JVM 间序列化（PythonRDD）；DataFrame/SQL 全 JVM 执行差距小；UDF 是大头（用 pandas_udf 缓解）。
- **Q Structured Streaming 的 watermark 和 Flink 的语义一样吗？** 概念相同（事件时间进度+迟到容忍）；Spark 的 watermark 还触发状态清理（过期窗口状态删除）。
- **Q 读 Kafka 的批与流的区别？** 批（read.format("kafka")，startingOffsets→endingOffsets 一次性）；流（readStream+checkpoint 持续）；场景二离线分析用的批模式。
