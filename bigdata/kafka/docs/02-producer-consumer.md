# 模块 16：Kafka 生产者与消费者实战

> **前置依赖**：模块 15（架构原理）
> **预估学习时间**：6 小时（文档 2h + 实验 3h + 面试题 1h）
> **配套代码**：`bigdata/kafka/src/`（5 个 Python 程序，全部可对真实集群运行）
> **配套实验**：`labs/lab-16-pipeline.md`

---

## 1. 生产者深入：从"发出去"到"发对"

### 1.1 发送链路：一条消息的编队过程

```
send() ──► 序列化器 ──► 分区器 ──► RecordAccumulator 攒批缓冲
                                        │ 按分区排队，攒满 batch.size 或 linger.ms 到期
                                        ▼
                              Sender 线程按 Broker 分组 ──► 网络发送 ──► Broker
                                        │
                              delivery 回调（成功/失败）
```

**分区器（Partitioner）的三级决策**——顺序性设计的落点：

1. 指定了 partition → 直接用；
2. 有 key → murmur2(key) % 分区数：**同 key 恒同分区**（保序的根基）；
3. 无 key → 粘性分区（KIP-480）：在"批"的时间尺度上粘住一个分区直到该批发送完成，既保住攒批效率又均匀分布。

通信场景的铁律：**一切有时序语义的数据必须带业务 key**。基站 KPI 用 `基站ID`、告警用 `网元+告警码`、流量记录用 `五元组哈希`。无 key 消息在消费端聚合时只能靠窗口乱序容忍。

### 1.2 幂等性（Idempotence）：Broker 端的去重

开启 `enable.idempotence=true` 后：Producer 启动时向 Broker 申请一个 **PID**（进程级、重启即变），每个 <PID, 分区> 的消息批携带从 0 递增的**序列号**。Broker 发现序列号重复 → 丢弃（重试去重）；发现序列号跳变 → 拒绝（保序）。约束：

- in-flight 请求窗口被限制为 **5**（Broker 端的滑动去重窗口）；
- 只保证**单分区、单会话**的 Exactly-Once；跨分区/跨会话要用事务；
- `acks` 被强制为 `all`——幂等与"等全部 ISR"是绑定语义。

**重试乱序问题的完整解释**（面试常考的细节）：未开幂等且 `max.in.flight>1` 时，批 1 失败重试、批 2 成功，Broker 看到批 2 在前批 1 在后——顺序被打乱。开幂等后 Broker 按序列号重排/拒绝，顺序恢复。

### 1.3 事务（Transactions）：跨分区原子性与端到端 Exactly-Once

事务解决幂等管不到的两个问题：**跨分区/跨 Topic 的原子写** 与 **消费-转换-生产闭环**（把"上游消费位移提交"和"下游生产"绑定成一个原子操作）。

```
initTransactions()          # 申请 PID + 恢复上次未完成事务（崩溃恢复的锚点）
    │
beginTransaction()          # 开事务
    │
produce() → 下游 Topic      # 先发数据（此时对外不可见）
send_offsets_to_transaction()# 消费位移并入事务
    │
commit / abort              # 提交：全部可见；中止：全部消失
```

两个配套机制（面试区分点）：

- **隔离级别**：消费端 `isolation.level=read_committed` 只读已提交消息；`read_uncommitted` 全读（脏读回滚数据）。
- **Fencing（僵尸防护）**：同一 `transactional.id` 的新实例初始化时会把旧实例的 PID "Bump 掉"（epoch+1），旧实例后续提交全部失败——防止两个"同名"生产者双写。

### 1.4 采集器实战模板（src/syslog_producer.py 的设计决策）

模拟"网管 Syslog/SNMP Trap 采集器 → Kafka"，四个决策点对应生产模板：① key=设备ID 保序；② `acks=all + 幂等 + 重试∞` 底线配置；③ `linger.ms=20 + batch.size=64KB + lz4` 吞吐配置；④ **失败消息落本地文件**——采集器最重要的容灾姿态：Kafka 短暂不可用时设备数据不能丢，恢复后重放。

## 2. 消费者深入：消费组与位移

### 2.1 消费组的两个不变式

- 组内：一个分区同时只被一个成员消费（并行度上限 = 分区数）；
- 组间：不同 Group 各自独立消费全量（一份数据，Flink 实时层、Spark 离线层、ES 归档层三个组互不干扰）。

### 2.2 再平衡（Rebalance）：触发、流程与新协议

**触发条件**四类：成员增减（发布/缩容/崩溃/处理超时被踢）、订阅 Topic 数变化、分区数变化、组成员订阅模式变化。

**两种协议**（当前面试热点）：

| | Eager（传统） | Cooperative（KIP-429，librdkafka 默认） |
|---|---|---|
| 流程 | 全员先撤销全部分区 → 集中分配 → 全员重新拉起 | 分两轮：只撤销被迁移的分区，其余成员不停 |
| 停顿 | 全组 Stop-The-World | 仅被迁移分区短暂停顿 |
| 兼容 | 所有版本 | Kafka 2.4+ |

**KIP-848 新消费组协议**（Kafka 4.x 默认）：把分配工作从"消费者组 Leader 客户端"移到 **Broker 端 Coordinator**，用增量心跳代替全量 JoinGroup/SyncGroup，再平衡时间从秒级降到百毫秒级、大群稳定性大幅提升。Apache Kafka 4.x 上 Python librdkafka 已支持 KIP-848 客户端（实验环境可用服务端新协议）。

**位移提交的三条铁律**（丢数与重复的总根源）：
1. **先处理、后提交**：先提交后处理 → 崩溃时这批消息被跳过（丢）；
2. 先处理后提交 → 崩溃时这批被重消费（重复）→ 由幂等消费兜底（`src/consumer_idempotent.py`）；
3. 提交语义是"**下一条**要读的 offset"：提交 `offset+1`。

### 2.3 位点的多义性与重置

- `committed`（组位移）/ `beginning` / `end` / **时间戳位点**（`--reset-offsets --to-datetime`）——排障时最常用"按时间重置"，例如"昨天 14:00 迁移事故期间的消息重放一遍"。
- `auto.offset.reset` 只在**无位移**时生效（新组或位移过期被删）——配错 earliest/latest 是常见事故源。

## 3. Kafka Streams 与 Kafka Connect（架构师视角）

- **Streams**：JVM 内嵌的流处理库，提供 KTable/KStream 抽象、窗口、Join、状态存储（RocksDB）与重建（changelog topic）。适合"轻量、随应用部署"的流处理；重量级有状态/低延迟场景选 Flink。面试一句话对比：**Streams 是库（随你的应用起停），Flink 是引擎（独立集群）**。
- **Connect**：标准化的源/汇连接器框架（JDBC/ES/HDFS/S3...），REST API 管理，Offset/配置/状态存 Kafka 内部 Topic 实现分布式伸缩。本项目模块 25 管道中 Filebeat→Kafka 用 Beats 系，Kafka→ES 用 ES Sink Connector。

## 4. 通信场景：设备日志采集管道的完整设计

```
基站/核心网 ──Syslog──► syslog-ng ─┐
             ──SNMP Trap─► 采集器  ─┼─► kafka: telecom-gnb-alarms (key=设备ID, RF=3/min.isr=2)
             ──NetFlow──► flow-collector ─► kafka: telecom-netflow
                                              │
                ┌─────────────────────────────┼─────────────────────────────┐
                ▼                             ▼                             ▼
        group=flink-realtime          group=spark-batch             group=es-archive
        （实时告警/CEP，模块19/20）      （历史分析，模块22）            （检索，ES Sink）
```

容量视角：单设备告警峰值 ~50 条/s，万级设备峰值 50 万条/s ≈ 250MB/s（单条 500B）→ 6 分区起步（单分区 50MB/s），采集器攒批 20ms 后单请求 100~500 条。

---

## 自检五问（答不上请回读对应小节）

1. 为什么"同一设备的告警"必须用设备 ID 做 key？（1.1 铁律）
2. 幂等生产者靠什么在 Broker 端去重？窗口为什么是 5？（1.2）
3. 事务如何实现"崩溃后不产生双写"？（1.3 Fencing）
4. Eager 与 Cooperative 再平衡的核心区别？KIP-848 又改了什么？（2.2）
5. "先处理后提交"为什么必然导致重复，靠什么兜底？（2.2 铁律）
