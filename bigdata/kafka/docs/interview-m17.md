# 模块 17 面试题：Kafka 集群运维与调优（7 道）

> 每道题：**面试官在考察什么** → **标准答案** → **加分回答** → **常见误区**

---

## Q1. Consumer Lag 持续增长，给出你的完整排查路径

**面试官在考察什么**：这是排障环节第一题。考察排查是否**有分层假设**（消费端→分区→集群）而不是上来就"加机器"。

**标准答案**（四步分层）：
1. **看形态**（`kafka-consumer-groups --describe`）：全分区均匀涨 → 消费端整体变慢；个别分区独高 → 数据倾斜（热点 key）；部分分区无人消费（CONSUMER-ID 空）→ 成员死亡/组异常。
2. **看消费端**：处理耗时是否上涨（下游 DB/ES 慢查询？Full GC？）、`max.poll.interval` 是否逼近、线程是否阻塞在重平衡。
3. **看分区**：倾斜分区的消息 key 分布（热点基站/大客户），单分区并行度上限=1，加消费者无效。
4. **看集群**：Broker 网络/磁盘 await、ISR 是否收缩（Fetch 变慢）。

处理：均匀涨→扩消费者（≤分区数）+优化处理；倾斜→key 加盐+下游二次聚合；暂时救火→`kafka-consumer-groups --reset-offsets --to-datetime` 跳过可丢弃区间（如可重算的统计类消息）。

**加分回答**：① 给出"跳过 vs 追平"的决策标准——统计类可跳过重算、状态类必须追平或重放（配合幂等消费）；② 说到 KIP-848 新协议下 Lag 指标口径变化与 exporter 兼容性；③ 结合通信场景：基站割接窗口告警风暴导致 Lag 冲高，预案是"告警管道临时降采样 + 事后重放"，而不是无限扩容。

**常见误区**：不加判断直接扩容（倾斜时无效）；把 Lag 阈值写死（不同组基线差异巨大）；不知道 reset-offsets 有 `--dry-run`。

---

## Q2. UnderReplicatedPartitions 告警了，怎么定位根因？

**面试官在考察什么**：URP 是 Kafka 最核心的健康指标，考察能否区分"Leader 侧问题"与"Follower 侧问题"。

**标准答案**：URP>0 表示存在副本未跟上 Leader。定位三步：① **先看是哪个 Broker 上的 Follower 掉队**（JMX `kafka.server:type=ReplicaManager` 按 broker 聚合）；② 掉队节点查三件事——**GC 停顿**（Full GC 期间不 fetch）、**磁盘 await**（iostat util 与 await，坏盘前兆）、**跨 Broker 网络**（机房间带宽打满）；③ 若是 Leader 所在节点问题，看 `RequestLatency` 与 Handler 线程池是否打满。修复：GC/磁盘/网络对应处理；副本数不足时先补副本（重分配）再修机器。

**加分回答**：① 提到 Kafka 2.4 的改进——follower 会主动告知 fetch 差距，配合 `replica.lag.time.max.ms` 时间标准，URP 抖动更少；② 演练思维：URP 告警演练（lab-17 实验一 pause Follower）应成为例行；③ 提到"URP 长期=1~2 个分区不恢复"的冷门场景：该分区的某副本所在磁盘文件句柄泄漏/目录损坏，需要 log dir 失败处理（`log.dir.failure.timeout.ms`）。

**常见误区**：URP 与 ISR 概念混用（URP 是 ISR 之外的副本数视角）；只重启了事不复盘；忽略"主机房带宽打满导致跨机房 Follower 集体掉队"这种系统性根因。

---

## Q3. 如何给 Kafka 集群做容量规划？分区数、副本数、Broker 数怎么定？

**面试官在考察什么**：架构师的核心日常工作，考察能否从业务量正向推导而不是拍脑袋。

**标准答案**（三层推导，见文档 1.1）：① 流量测算：峰值消息速率×消息大小=入带宽，×消费组数=出带宽；② 副本放大：入带宽×RF=集群磁盘写入；③ 节点数：集群写入÷单节点安全水位（磁盘顺序吞吐的 30%）。分区数=max(入/单分区写入, 出/单分区消费)×1.5 冗余，且对齐下游并行度。副本数：网管告警类 RF=3+min.isr=2，日志类可 RF=2。最后验证：内存（堆 6GB 内+页缓存）、网卡（入×RF×1.2）。

**加分回答**：① 给出通信行业实例：10 万基站 KPI 1GB/s 入、3 消费组 → 3GB/s 出，RF=3 → 3GB/s 集群写，单节点双 NVMe 500MB/s×30% → 20 节点、24~36 分区；② 强调"分区只能增不能减"带来的扩分区乱序窗口要跟下游约定；③ 提到机架感知 `broker.rack` 与跨机房部署时 Raft controller 的延迟敏感性（KRaft 新约束）。

**常见误区**：只算磁盘不算网卡与副本复制放大；分区数与 Flink 并行度不匹配造成数据倾斜；RF=2 的"省 1/3 磁盘"在 min.isr=2 时等于自缚手脚。

---

## Q4. 生产环境 Kafka 为什么要关闭 auto.create.topics.enable？Topic 生命周期应该怎么管？

**面试官在考察什么**：治理意识——Topic 是成本与故障源，考察是否有平台化治理视角。

**标准答案**：自动建 Topic 的三宗罪：① 参数不可控（默认 RF=1、分区=1），出错难排查；② 拼写错误的 Topic 名会静默创建垃圾 Topic，磁盘被慢慢吃掉；③ 绕过了治理（审批/配额/保留策略）。正确姿势：**代码化声明**（IaC：脚本/GitOps 统一创建，本项目 `create-topics.sh` 即此模式）或接入自助平台（Schema Registry + 审批流），配 `auto.create.topics.enable=false` + `delete.topic.enable=true`（可清理）+ 定期审计孤儿 Topic。

**加分回答**：① 提到 Schema 治理：Confluent Schema Registry 强制 Avro/Protobuf 演进兼容性，防"上游改字段下游炸"；② 保留策略分级：热数据 7 天 + 冷数据压 Parquet 入对象存储（Tiered Storage 是 Kafka 3.6+ 的新选项，面试新考点）；③ 配额（`kafka-configs --alter --add-config 'producer_byte_rate'`）防大客户把集群打满——多租户治理必备。

**常见误区**：只说"防拼写错误"这一层；不知道删除 Topic 是异步的且默认 `delete.topic.enable` 行为随版本变化；没听过配额机制。

---

## Q5. 滚动升级 Kafka 集群的完整流程与风险控制是什么？KRaft 集群有什么特殊点？

**面试官在考察什么**：生产运维的"高危操作"题——升级是最容易出全集群事故的动作。

**标准答案**：① 前置：确认当前 inter-broker/message 协议版本、ControllerQuorumVoters 版本，全量备份元数据（KRaft 下备份 `@metadata` 与 meta.properties）；② 升级顺序：**逐台 Broker 滚动**（每次一台，等 ISR 恢复再下一台），KRaft 集群 **Controller 节点先于 Broker**；③ 分阶段切换参数：先升软件（新协议默认关闭）→ 观察 → 改 `inter.broker.protocol.version` → 改 `log.message.format.version`（3.0+ 已合一）；④ 回滚窗口：协议版本未提升前可回滚，一旦提升**不可回滚**——这是风险控制的核心闸门。全程监控 URP/RequestLatency/Controller 日志。

**加分回答**：① 指出 KRaft 的 `kafka-metadata-quorum.sh describe` 用于升级期间观察 Controller 仲裁健康；② 混合版本期间 Client 行为：新 Client 对旧 Broker 用旧 API（API 版本协商），所以客户端升级可以先行；③ 通信场景的窗口选择：避开月底出账/节假日保障期，升级前冻结配置变更。

**常见误区**：一口气改协议版本（不可回滚）；不知道 Controller 先于 Broker；升级中不看 URP 就推进下一台。

---

## Q6. 机房级容灾怎么做？MirrorMaker 2 的原理与坑是什么？

**面试官在考察什么**：容灾架构题，通信行业同城双活/异地容灾是硬需求。

**标准答案**：MM2（KIP-382）基于 KafkaConnect 框架，用 `MirrorSourceConnector` 远端消费→本地生产，特性：① **Topic 命名带前缀**（`src.topic`）防环；② **循环复制检测**：消息带远端集群信息（source cluster alias）防 A→B→A 死循环；③ 位移翻译：`--checkpoint` 把源组位移映射到目标集群，故障切换后消费组能"接着读"；④ 语义：默认 **at-least-once**（异步复制），不保证跨集群有序与零丢失——RPO=复制延迟。架构上同城双机房一般"双活集群+MM2 互备"，异地容灾 RPO 放宽。

**加分回答**：① 指出 failover 的真正难点：**位移翻译的窗口**——切换瞬间未翻译的位移会重放/丢失一段，配合消费端幂等消化；② 客户端切换手段：DNS/服务发现切 bootstrap servers，或 `bootstrap.servers` 写多集群前置 LB；③ 提到 MM2 与旧 MirrorMaker 1 的区别（MM1 无位移翻译、无循环检测，已废弃）；④ 通信场景：省级网管平台异地容灾，告警 Topic 同步优先级最高，KPI 明细允许分钟级 RPO。

**常见误区**：以为 MM2 能做到强一致（异步复制本质）；没考虑 offset 翻译窗口；双写双集群（应用层双写）与 MM2 异步复制的取舍不分。

---

## Q7. 如何监控一个 Kafka 集群的"端到端健康"？给出你的大盘设计

**面试官在考察什么**：可观测性设计能力——从指标到大盘到告警的完整链路，这是 SRE/架构师岗的加分题。

**标准答案**（大盘分层）：① **集群层**：ActiveControllerCount（=1）、OfflinePartitionsCount（=0）、URP（=0）、Broker 存活数、请求 p99（Produce/Fetch）、网络/IO 线程空闲率；② **Topic/分区层**：入出速率、分区 Leader 分布均衡度、磁盘水位；③ **消费层**：各组 Consumer Lag（分区级）、再平衡次数、消费请求延迟；④ **KRaft 层**：Quorum Leader/Lag、元数据日志大小。告警分级：Offline/双 Controller=P0 电话；URP 持续 5 分钟=P1；Lag 超 5× 基线=P2。

**加分回答**：① 指出"业务层黄金指标"：**消息端到端延迟**（生产时间戳 vs 消费时间戳的分位数）比 Lag 更接近用户感受，用 Kafka 自带 `ActiveControllerCount` 之外的 `records-lag` + 客户端埋点；② 工具链：JMX Exporter（进程内全量）+ Kafka Exporter（消费组/Lag）+ Burrow（Lag 滑动窗口评估，减少误报）；③ 结合通信场景：大盘叠"割接日历"图层——Lag 尖峰与割接窗口对齐即预期内，避免告警疲劳。

**常见误区**：只配了 Broker 指标没有消费组视角；Lag 告警没有基线（用绝对值）；大盘有指标没有"下一步去哪查"的 runbook 链接。
