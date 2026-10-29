# 模块 16 面试题：Kafka 生产者与消费者实战（8 道）

> 每道题：**面试官在考察什么** → **标准答案** → **加分回答** → **常见误区**

---

## Q1. Kafka 幂等生产者的实现原理是什么？它解决不了哪些问题？

**面试官在考察什么**：区分"知道幂等这个词"与"理解 PID/序列号机制"；追问"解决不了什么"能筛掉背题者，引向事务与业务幂等的边界。

**标准答案**：开启 `enable.idempotence` 后，Producer 启动时向 Broker 申请 **PID**；每个 <PID, 分区> 的消息批携带从 0 递增的**序列号**。Broker 维护每个 <PID, 分区> 最近 5 个批的序列号窗口：重复序列号 → 丢弃（重试去重）；序列号跳变 → 拒绝（防止乱序）。因此它同时解决重试重复与重试乱序。解决不了：① 跨分区/跨会话（PID 重启即变）→ 需事务；② 消费端处理动作的副作用幂等 → 需业务主键去重。

**加分回答**：① 指出 in-flight 被限制为 5 的原因——Broker 的滑动窗口去重以"最近 5 批"为界，窗口外无法判定；② 指出 `acks` 被强制为 all（语义绑定）；③ 结合通信场景：网管采集器幂等开启后，网络抖动重试不会在网管里产生重复告警，但"采集器进程重启再发"仍需设备侧 event_id 去重。

**常见误区**：认为幂等=事务（粒度完全不同）；不知道 in-flight=5 约束，配 1000 还奇怪为什么报错；以为幂等能防"消费者重复"。

---

## Q2. 设计一个"消费-转换-生产"端到端 Exactly-Once 管道，关键点有哪些？

**面试官在考察什么**：Kafka 事务的综合运用能力——位移与下游消息原子绑定、隔离级别、Fencing、失败恢复，一个都缺不了一份工作。

**标准答案**（五个关键点）：① 生产者配唯一 `transactional.id`，启动时 `initTransactions`——Broker 恢复未完成事务并 Fencing 掉旧实例；② 消费者关自动提交，`enable.auto.commit=false`；③ 每批处理：begin → produce 下游 → `send_offsets_to_transaction`（位移并入事务）→ commit/abort；④ 下游消费者 `isolation.level=read_committed`，只读已提交；⑤ 上游位移回滚后消息会被重消费——处理逻辑必须幂等（业务主键唯一表），这是闭环外的最后一道防线。

**加分回答**：① 点出"位移+消息原子性"为什么重要——分别提交时崩溃会造成"消息发了位移没提（重复消费）"或"位移提了消息没发（丢失）"；② 说出 LSO（Last Stable Offset）概念：read_committed 消费者只能读到 LSO，未决事务会卡住 LSO，**长事务会拖垮下游消费**（生产红线：事务要短）；③ 提到与 Flink Exactly-Once 的关系——Flink 2PC Sink 的 preCommit/commit 就是这套机制的引擎级封装（模块 20 展开）。

**常见误区**：只答"开事务就行"漏掉隔离级别；不知道 send_offsets_to_transaction 的存在（这是"端到端"三个字的全部含义）；用长事务跑大批量数据。

---

## Q3. 消费组什么时候触发再平衡？如何避免"再平衡风暴"？

**面试官在考察什么**：生产事故高频区。考察能否列出全部触发条件、说出 STW 机制、给出生产参数级解法——这是区分"用过"和"运维过"的分水岭。

**标准答案**：触发四类：① 成员增减（上线/下线/**处理超时被踢**）；② 订阅 Topic 集合变化；③ 分区数变化；④ 订阅正则变化。风暴机理：一次再平衡 → 全组停止消费 → 处理中的消息超时 → 更多成员被踢 → 再平衡连环触发。解法：① 用 cooperative-sticky（增量协作，只迁移被撤销分区）；② 控制 `max.poll.interval.ms` > 最大单批处理耗时，`max.poll.records` 调小；③ 心跳参数保持默认（heartbeat 在后台线程，调大无益）；④ 避免频繁发布 + 优雅关闭（close() 主动 leave）。

**加分回答**：① 指出 Java 客户端与 librdkafka 的差异：librdkafka 心跳在后台线程、默认即 cooperative-sticky，Java 客户端 poll 驱动心跳、默认 range/roundrobin——跨语言面试题；② KIP-848（4.x 默认）把分配权收到 Broker 端 Coordinator，增量心跳替代全量 Join/Sync，大组再平衡从秒级到百毫秒级；③ 排障手法：`kafka-consumer-groups.sh --describe` 看 COORDINATOR 与成员状态，Broker 日志搜 PrepareRebalance 频次。

**常见误区**：说"消费者宕机才触发"（漏掉处理超时这个最大元凶）；把 max.poll.interval.ms 和 session.timeout.ms 混为一谈；认为调大 heartbeat interval 能"减少再平衡"。

---

## Q4. `enable.auto.commit=true` 会丢数据吗？什么时候会？完整的位移提交策略怎么定？

**面试官在考察什么**：位移语义的精确理解——这是消费端丢数/重复的总根源，也是网管类强一致场景的必答题。

**标准答案**：会。自动提交按 `auto.commit.interval.ms`（默认 5s）周期提交**当前 poll 返回的最大 offset+1**，与业务处理完成与否无关。丢数场景：poll 到消息 → 自动提交触发 → 业务处理前进程崩溃 → 重启从已提交位置继续，这批消息被跳过。完整策略：强一致场景手动提交（处理完→同步/异步提交 offset+1）；吞吐场景异步提交 + 定期同步兜底；rebalance revoke 回调中必须同步提交被撤销分区的位移；处理后崩溃导致的重复由幂等消费兜底。

**加分回答**：① 提交的是"下一条要读的 offset"（offset+1 语义），差一位就是多处理/漏处理一条；② 指出异步提交失败要放进重试队列，且注意"更大位移覆盖"原则（旧位移提交失败后新位移成功即可放弃旧的）；③ 结合场景：告警入库类管道必须手动提交 + 幂等；KPI 明细统计类可以接受自动提交 + 下游窗口容重。

**常见误区**：认为自动提交"至少不丢"（顺序恰好相反）；不知道 revoke 时要手动补提交；offset+1 语义说不清。

---

## Q5. 分区分配策略有哪些？Reassign Partitions 和 Consumer Assignment 有什么区别？

**面试官在考察什么**：概念精确性——两个"分配"一字之差，一个是客户端消费分工，一个是 Broker 端副本迁移，混用说明没运维过集群。

**标准答案**：Consumer Assignment（消费分配）：range/roundrobin/sticky/cooperative-sticky，决定**分区→消费者成员**的映射，由消费组协调。Reassign（分区重分配）：`kafka-reassign-partitions.sh` 生成迁移计划，把**分区副本**从一组 Broker 搬到另一组（数据真实拷贝），用于扩容 Broker 后的负载再均衡、坏盘迁移；过程中由 Controller 管理 ReassignPartitions 事件，限流用 `--throttle` 参数，完成后清理计划（注意 `--verify` 直到确认完成，否则限流配置残留）。

**加分回答**：① 消费分配的 sticky 语义：再平衡时尽量保留原映射，减少分区→成员的跳变（对应消费者本地缓存/状态失效成本）；② 重分配的实操要点：分区迁移期间该分区 Leader 切换会造成短暂延迟尖峰；Cruise Control（KCC）是生产自动化的主流方案，按负载画像生成Proposal；③ 通信场景：新上架 Broker 空载时 KCC 自动搬副本，机房腾退时按机架维度生成迁移计划。

**常见误区**：把两个"分配"混为一谈；不知道重分配要限流（打爆集群带宽的真实事故）；以为扩了 Broker 数据自动均衡（Kafka 不会自动搬副本）。

---

## Q6. Kafka Streams 与 Flink 怎么选？各适合什么场景？

**面试官在考察什么**：流处理技术选型的架构判断力——考的是边界认知而非站队。

**标准答案**：Streams 是**库**：嵌入你的 JVM 应用，随应用部署，无独立集群；提供 KStream/KTable、窗口、Join、RocksDB 状态 + changelog 重建、与 Kafka 事务打通的 EOS。适合：轻量转换/过滤/ enrichment、随微服务部署、团队 Kafka 栈统一。Flink 是**独立引擎**：独立集群调度、事件时间/Watermark 完整体系、CEP、更大的状态与更精细的容错（分布式快照）、SQL/CEP/PyFlink 全家桶。适合：复杂事件处理、大状态作业、有专门流处理团队的平台。选型一句话：**转换管道用 Streams，平台级流计算用 Flink**。

**加分回答**：① 通信运维平台的具体分工：设备数据 enrichment（查维度表补齐网元属性）用 Streams 内嵌在采集服务；SLA 违规检测/CEP 攻击模式匹配用 Flink（本项目场景一/二正是这个分工）；② 指出两者 EOS 的实现差异：Streams 依赖 Kafka 事务（KIP-447 后按输入分区 fencing），Flink 用两阶段提交 Sink + Checkpoint；③ 提到 KIP-848 对 Streams 消费层的影响与 Flink Kafka Source 的 Envelope 协议演进。

**常见误区**：无理由站队 Flink"更高级"；不知道 Streams 也有状态与窗口能力；忽略团队栈与运维成本这两个真实约束。

---

## Q7. 生产者的 linger.ms 和 batch.size 如何权衡？给出一个"告警采集"场景的调参过程。

**面试官在考察什么**：延迟-吞吐权衡的量化思维；能否从业务 SLA 反推参数，而不是背"越大越好"。

**标准答案**：两者共同决定攒批时机：batch.size 到 OR linger.ms 到，先到先发。linger.ms=0 立即发（低延迟、小请求、低吞吐）；调大攒批 → 更大请求 → 更高吞吐（磁盘顺序写量增大、网络包数减少），代价是端到端延迟增加。告警采集调参过程：① 业务 SLA：告警从设备到平台的可见性预算 1s，采集环节可容忍 50ms 延迟 → linger.ms=20 安全；② 消息大小 ~500B，目标单请求 64KB → batch.size=65536（约 128 条）；③ 压测验证：发送 TPS、请求平均大小、p99 延迟三项指标对照，若 TPS 未达标优先加压缩 lz4 而非继续加大 linger；④ 队列满（buffer.memory）时 produce 阻塞，采集器需背压告警。

**加分回答**：① 指出两者与压缩的关系：攒批后整批压缩，压缩比更高——"攒批+压缩"是乘法效应；② 给出异常路径的权衡：linger 越大，进程崩溃时未发送批次丢失越多（无 WAL 时），所以告警这类"单条都很关键"的场景 linger 不宜超过 50ms；③ 提到幂等+重试与批的相互作用：批是重试的原子单位，批越大重试代价越大。

**常见误区**：只背参数值不讲 SLA 推导；把 batch.size 理解为"条数"（是字节）；忽略 buffer.memory 打满时 produce 的阻塞行为。

---

## Q8. 一个消费组怎么消费多个 Topic？订阅模式有什么坑？

**面试官在考察什么**：实际工程细节——多 Topic 订阅在网管平台是常态（告警 Topic + KPI Topic 同组消费），细节坑能检验真实经验。

**标准答案**：三种方式：① `subscribe(list)`：订阅多个 Topic，组按每个 Topic 的分区数分配；② `subscribe(Pattern)`：正则订阅，**新增匹配 Topic 自动加入订阅**（需要 metadata.refresh 间隔）；③ `assign()`：手动指定分区（脱离组管理，位移仍可提交到组）。坑：① 正则订阅 + auto.create 场景下，脏 Topic 意外被消费；② 组内成员订阅列表不一致会触发"订阅不一致"异常（Java 客户端直接报错）；③ range 策略下多 Topic 分区数不均 → 分配倾斜，改 sticky/roundrobin；④ assign 模式没有再平衡保护，成员宕机分区无人接管。

**加分回答**：① 通信平台实践：告警与 KPI 处理逻辑差异大，**不同组分开消费**（组间广播、组内分工），而不是一个组订阅多个 Topic 混流处理——隔离故障域；② 指出 `partition.assignment.strategy` 在多 Topic 倾斜时的调整；③ metadata.max.age.ms 对正则发现新 Topic 延迟的影响。

**常见误区**：一个组混订所有 Topic 导致处理逻辑耦合；不知道订阅不一致会拒绝入组；assign 与 subscribe 混用导致位移错乱。
