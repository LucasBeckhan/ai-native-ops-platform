# 模块 19：Flink 流处理实战——Kafka 集成、窗口聚合与 CEP

> **前置依赖**：模块 18；Kafka 集群与 Topic（模块 15）
> **预估学习时间**：6 小时
> **配套代码**：`bigdata/flink/src/main/java/com/teleops/flink/m19/`（两个作业，真集群验证含 DDoS 告警输出）
> **配套实验**：`labs/lab-19.md`

---

## 1. Kafka Connector：生产级 Source/Sink 配置

```java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafka-1:9092,...")   // 容器网络用服务名（宿主机才用 localhost:39092）
    .setTopics("telecom-gnb-kpi")
    .setGroupId("flink-kpi-alert")             // 位点按 groupId 存 Kafka
    .setStartingOffsets(OffsetsInitializer.latest())  // earliest=全量回放 / latest=只看新数据
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();
```

四个生产要点（均来自本项目实现）：
1. **位点管理**：Checkpoint 成功时把 offset 并入状态原子提交（`commit.offsets.on.checkpoint`），恢复时从 checkpoint 位点重放——这比"自动提交"可靠一个量级；
2. **动态分区发现**：Topic 扩分区后 Source 自动感知（`partition.discovery.interval.ms`）；
3. **watermark 与 idleness**：`withIdleness` 防空闲分区冻结全局进度（模块 18 实测教训）；
4. **Sink 交付语义**：默认 AT_LEAST_ONCE；`EXACTLY_ONCE` 走 Kafka 事务（两阶段提交，代价：下游必须 read_committed + 首条可见延迟增大）。本项目注释了两种选择的取舍。

## 2. 实时 KPI 告警作业（KafkaKpiAlertJob）

```
telecom-gnb-kpi ──► KafkaSource ──► 30s 事件时间滚动窗口
                                     ├── AggregateFunction 增量算均值
                                     └── ProcessWindowFunction 阈值判定
                                          └── 低于 700Mbps → 告警 JSON
                                               ├── print（实验观察）
                                               └── KafkaSink → telecom-alerts（Agent 的输入）
```

设计决策三则：① 阈值判定放 `ProcessWindowFunction`（需要窗口元数据拼告警时间区间）；② 脏数据兜底返回"永不告警"占位（教学简化；生产用 `sideOutputLateData`/死信流，见模块 20）；③ 告警是**结构化 JSON**（type/device_id/window/severity）——下游 Agent 直接可消费，这是"数据面与智能面解耦"的接口契约。

## 3. CEP：复杂事件处理（CepIntrusionJob）

### 3.1 Pattern 三要素

```java
Pattern.<FlowEvent>begin("syn")
    .where(flags == "S")            // ① 条件：哪些事件参与匹配
    .timesOrMore(5)                 // ② 量词：至少 5 次（开放量词！）
    .within(Time.seconds(60));      // ③ 时间约束：60 秒内（防"全天凑数"误报）
```

**本项目实测得出的两个关键语义**（面试极易答错）：
1. **开放量词的匹配输出时机**：`timesOrMore` 无法知道"不会再有更多匹配"，所以匹配结果在 **watermark 推过 `within` 窗口末端**时才输出。实验中 SYN 洪水注入后 8 秒内无输出，注入 ts=+65s 的事件推进 watermark 后立即输出 `{"attack_type":"SYN_Flood","syn_count":6,...}`——这不是 bug，是语义。
2. **`.consecutive()` 是严格连续**（事件必须紧挨着），默认的松散连续（`allowCombinations` 才是"可夹其他事件"）。攻击检测通常用默认松散连续（攻击者 SYN 之间夹杂正常流量很正常）。

### 3.2 常见攻击模式与 Pattern 对照

| 攻击类型 | Pattern 思路 |
|---|---|
| SYN Flood | 同 src_ip，`flags=S`，60s 内 ≥5 次（本项目实现） |
| UDP Flood | 同 src_ip+dst_ip，60s 内包数超阈值（量词+阈值条件组合） |
| 慢速攻击 | `next("slow").within(分钟级)` + 超时事件（TimedOut 旁路） |
| 密码爆破 | 同 src_ip，auth_fail 事件 5 分钟 ≥10 次 |

### 3.3 CEP 的代价与边界

CEP 的 NFA（非确定有限自动机）状态随"部分匹配"数量增长：宽匹配模式（如 `oneOrMore` + 宽条件）在高速流上会状态爆炸。**工程纪律**：① 条件尽量收紧（先 keyBy 缩小匹配域）；② `within` 必配；③ 高基数 key（每 IP 一个 NFA）要配状态 TTL 或改用窗口聚合近似实现。这些取舍在模块 20 的调优清单里展开。

## 自检三问

1. EXACTLY_ONCE 的 KafkaSink 为什么会增大首条可见延迟？（1：两阶段提交 preCommit→commit）
2. timesOrMore 为什么不能"第 5 个事件到达就输出"？（3.1 语义 1）
3. CEP 状态什么时候爆炸？三条工程纪律？（3.3）
