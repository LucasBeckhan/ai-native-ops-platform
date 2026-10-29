# 模块 20：Flink 调优与运维——完整监控管道

> **前置依赖**：模块 18/19
> **预估学习时间**：6 小时
> **配套代码**：`bigdata/flink/src/main/java/com/teleops/flink/m20/EsAlertPipelineJob.java`（真集群验证：ES 索引 113 条告警）
> **配套实验**：`labs/lab-20.md`

---

## 1. 反压（Backpressure）：流处理的心率

反压 = 下游处理速度跟不上上游产生速度，压力沿 DAG 逆向上传直到 Source 降速。**它是自我保护机制**，但持续反压 = 吞吐瓶颈。

**定位方法（Web UI 三步法，面试常考）**：
1. Web UI → 作业 → 反压状态页：`HIGH` 的算子标红；
2. 看各算子 **busy/backPressured/idle** 时间占比：瓶颈在 busy 最高的算子；
3. 开火焰图（本 compose 已配 `rest.flamegraph.enabled: true`）：CPU 热点函数一目了然。

**常见根因与解法**：

| 根因 | 特征 | 解法 |
|---|---|---|
| 用户函数慢（重解析/同步 IO） | busy 高、火焰图热点集中 | 优化函数；同步 IO 改 Async I/O |
| 数据倾斜（某 subtask 独忙） | 个别 subtask busy 100% 其余 idle | 两次聚合/加盐（见第 2 节） |
| 窗口状态过大 | Checkpoint 变慢 + GC 频繁 | 增量聚合、RocksDB、状态 TTL |
| 并行度不足 | 全部 subtask 均匀高 busy | 提高并行度（≤分区数） |

## 2. 数据倾斜：两次聚合与加盐

```
现象：keyBy(deviceId) 后，热点基站（大客户/中心机房）的 subtask 独扛 80% 流量。

解法（两次聚合）：
第一跳: keyBy(deviceId + "#" + salt(0~9))  → 10 个子流各算部分聚合（打散热点）
第二跳: keyBy(deviceId) + ProcessWindowFunction → 合并 10 份部分结果
代价：多一层网络 shuffle；适用：聚合型指标（求和/均值），不适合去重类。
```

通信场景的真实倾斜源：告警风暴中的单基站、割接时段的批量设备、大客户切片。**先确认是倾斜再动手**（看 subtask 级 Records Received 分布，别把均匀慢也当倾斜治）。

## 3. Checkpoint 调优

| 参数 | 本项目值 | 调优逻辑 |
|---|---|---|
| interval | 10s | 越小恢复越快，但快照开销占比越高 |
| timeout | 60s | 大状态作业要加大（RocksDB 快照慢） |
| minPauseBetween | 1s | 防止检查点背靠背 |
| 增量 Checkpoint | 实验关闭 | RocksDB 必开（只传 SST 差量，大状态必备） |

**Checkpoint 失败排查序列**（排障题高频）：① 看 Coordinator 日志的失败原因（超时？Task 失败？）；② 超时 → 查反压（Barrier 传输被反压拖慢——Barrier 对齐时间可在 Web UI 看占比）→ 开 unaligned checkpoint（牺牲吞吐保 Checkpoint 成功）；③ 状态大 → 增量 + 加大 timeout；④ 存储 IO 慢 → 状态后端换更快的盘/独立盘。

## 4. 监控管道实战（EsAlertPipelineJob 的三层教训）

```
Kafka/注入源 → CEP(链路抖动检测) ─┬─ KafkaSink(telecom-alerts, Agent 消费)
                                  └─ EsBulkSink(REST _bulk 直写 ES)
```

本项目实现里埋了三个"生产对比点"（代码注释均已标注）：

1. **SourceFunction vs 新 Source API**：实验用注入源最直观；生产用统一的新 Source 接口（分片/位点/水位内建）；
2. **自研 REST Sink vs 官方 ES 连接器**：教学实现只做"攒批 10 条/2 秒 + HTTP POST"；官方连接器多出 **flushOnCheckpoint**（Checkpoint 时强制刷出，保证 ES 侧与 Flink 状态对齐）、退避重试、BulkProcessor 批量调优——"Sink 的本质是攒批+刷出+重试"三件事；
3. **Event Time 需要显式注入**：注入源路径最初没配 WatermarkStrategy，CEP 永不输出（实测踩坑）——**SourceFunction 不自带时间语义**，这是新手最高频事故。

**指标接入 Prometheus**（实验步骤见 lab）：把 `/opt/flink/opt/flink-metrics-prometheus-*.jar` 复制到 `plugins/` 并在 flink-conf 加 reporter 配置，Prometheus 即可抓取 9250 端口（docker/monitoring/prometheus.yml 已预置 flink job）。

## 5. Exactly-Once 端到端：缺的最后一公里

```
恢复后状态不丢不重（Checkpoint 保证，模块 18 实证）
    + Source 位点重放（Kafka 位点入状态）
    + Sink 幂等/事务（ES 用 id 幂等写 / Kafka 用事务）  ← 本节补的最后一公里
    = 端到端 Exactly-Once
```

| Sink 类型 | Exactly-Once 手段 |
|---|---|
| Kafka | 事务（两阶段提交） |
| ES | 文档 _id 幂等写（重放覆盖同文档） |
| JDBC | Upsert / 唯一键幂等 |
| 文件 | 提交协议（pending→committed 重命名） |

面试一句话：**"Flink 的 Exactly-Once 是状态语义，端到端靠 Source 位点 + Sink 幂等/事务共同达成"**——把这句话展开讲清楚的人，流处理功底过关。

## 自检三问

1. busy/backPressured/idle 三个指标怎么组合定位瓶颈？（1）
2. 什么时候开 unaligned checkpoint？代价是什么？（3：反压时保 Checkpoint，牺牲吞吐）
3. 为什么自研 REST Sink 不是 Exactly-Once？（4：无 flushOnCheckpoint，重放产生重复文档）
