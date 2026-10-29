# 模块 15：Kafka 架构原理

> **前置依赖**：无（从零讲起）
> **预估学习时间**：文档精读 2 小时 + 实验 2 小时 + 面试题复盘 1 小时
> **配套代码**：`bigdata/kafka/scripts/`（存储探查、顺序写基准、ISR 观察）
> **配套实验**：`bigdata/kafka/labs/lab-15-storage-and-isr.md`

---

## 1. Kafka 是什么，为什么通信行业离不开它

Kafka 是一个**分布式提交日志（Commit Log）系统**，对外表现为"可持久化的消息队列 + 流式存储平台"。它解决的核心问题是：**让"数据生产方"和"数据消费方"解耦，且吞吐量能达到百万级消息/秒**。

在通信网络里，一台 5G 基站每秒上报几十条性能指标，一个省 10 万台设备就是每秒数百万条记录；核心网网元告警、NetFlow 流量记录、信令日志都以流的形式持续产生。网络运维平台必须先有一个"洪水坝"——先稳定接收、按序落盘，再让异常检测、网管入库、告警推送等多个下游各自消费。这就是 Kafka 在本项目中的角色：`设备/gNB 采集器 → Kafka → Flink 实时计算 / Spark 离线分析 / ES 检索`。

术语先立起来（后文全部会用）：

| 术语 | 一句话定义 | 类比 |
|---|---|---|
| Broker | 一个 Kafka 服务进程/节点 | 仓库里的一台货架服务器 |
| Topic | 消息的逻辑分类名 | 仓库里的"品类"标签 |
| Partition | Topic 的物理分片，**有序性的最小单位** | 把品类再切成多个货架通道 |
| Replica | 同一 Partition 的多个拷贝（1 Leader + N Follower） | 每个通道的备份货架 |
| ISR | 与 Leader 保持同步的副本集合（含 Leader 自身） | "在线且跟得上进度"的备份名单 |
| Offset | 消息在 Partition 内的唯一递增编号 | 通道里的货架序号 |
| Producer/Consumer | 生产者/消费者 | 送货人/取货人 |
| Consumer Group | 一组共同消费一个 Topic 的消费者实例 | 一个班组共同取货，组内分活 |

## 2. 集群架构与 KRaft 元数据体系

### 2.1 三层逻辑视图

```
Topic: telecom-gnb-kpi  (分区数=3, 副本因子=3)
├── Partition 0:  [Leader→Broker1] [Follower→Broker2] [Follower→Broker3]   offsets: 0..1048575
├── Partition 1:  [Leader→Broker2] [Follower→Broker3] [Follower→Broker1]   offsets: 0..1048502
└── Partition 2:  [Leader→Broker3] [Follower→Broker1] [Follower→Broker2]   offsets: 0..1048590
```

三条铁律，请直接背下来（面试第一题就在这里）：

1. **分区是并行度的单位**：生产者按分区键把消息打散到多个分区，多个消费者并行消费——吞吐量随分区数水平扩展。
2. **分区是顺序性的边界**：**只在分区内**保证写入顺序与读取顺序；跨分区无序。所以"同一基站的所有 KPI"必须用 `基站ID` 作为消息 key，落到同一分区，才能保证时序分析不出乱序。
3. **副本是可用性与持久性的单位**：同一分区的一个副本挂掉，其他副本顶上；`acks=all` + `min.insync.replicas≥2` 才敢谈"消息不丢"。

### 2.2 KRaft：用 Raft 协议管理元数据（重点，面试高频）

Kafka 2.8 之前，元数据（哪些 Broker 活着、Topic→分区→副本的映射、Controller 是谁）存放在 **ZooKeeper**，架构上"两套分布式系统"带来双倍运维与一致性问题。Kafka 3.3 起 KRaft（Kafka Raft）模式转正，**4.0 起彻底移除 ZooKeeper**——本项目使用的 Apache Kafka 镜像就是纯 KRaft。

KRaft 把节点分成两种角色（可以合一，叫 combined 模式）：

- **Controller 节点**：组成一个 Raft 仲裁组（quorum），用 **Raft 共识算法**选举 Leader Controller、把元数据作为**日志**复制到每个 Controller 节点，存进内置的 `@metadata` 内部主题。奇数个节点（3/5）容忍 (n-1)/2 个故障。
- **Broker 节点**：负责真正的数据读写，通过 Raft 客户端协议从 Controller Raft 组**拉取元数据日志**，在本地重放（replay）得到完整元数据视图。

```
      KRaft 元数据面（Raft 共识）                    数据面
┌──────────┐ ┌──────────┐ ┌──────────┐      ┌─────────────────────────┐
│Controller│ │Controller│ │Controller│      │ Broker1   Broker2  Broker3│
│  Leader  │→│ Follower │→│ Follower │ ───→ │ 拉取并重放元数据日志        │
└──────────┘ └──────────┘ └──────────┘      │ /var/lib/kafka/@metadata │
        元数据变更追加为日志，多数派落盘才算提交     └─────────────────────────┘
```

**KRaft vs ZooKeeper 的面试标准答案要点**：① 元数据从"外部 ZK 双写"变为"内部 Raft 日志"，故障切换从秒级 ZK 会话超时变为亚秒级本地日志追平；② 控制面事件从"逐条 API 通知"变为"日志快照+增量重放"，百万分区场景下 Controller 冷启动从小时级降到分钟级（这是 ZK 模式被诟病的核心痛点）；③ 运维上少维护一套 ZK，配置/安全/监控收敛为一套。加分点：能指出 KIP-858 目录级快照、元数据日志与数据日志共用同一套 Segment 存储格式。

### 2.3 一次写入的完整旅程（把组件串起来）

```
Producer                          Broker                                Consumer
   │ 1.向任意 Broker 请求              │                                      │
   │   telecom-gnb-kpi-0 的 Leader    │                                      │
   │ ◄──── 元数据路由：去 Broker1 ──── │                                      │
   │ 2.发送 Produce 请求(含 acks)      │ 3.写入页缓存→顺序追加 .log            │
   │ ───────────────────────────────► │ 4.等待 ISR 内 min.insync.replicas    │
   │                                  │   个副本确认（Follower 从 Leader fetch）│
   │ ◄──── 5.返回 offset+时间戳 ────── │ 6.后台线程落盘、构建 .index/.timeindex│
   │                                  │                                      │
   │                                  │ ◄── 7.消费者 Fetch 请求(带 offset) ── │
   │                                  │ ── 8.返回消息批（零拷贝直出页缓存）──► │
```

关键认知：**Kafka 的"持久化"发生在第 3 步（页缓存）与第 6 步（异步刷盘）之间**——这意味着单机掉电理论上有丢数窗口，Kafka 靠**多副本冗余**而不是单机 fsync 保证持久性。这是"Kafka 为什么快"与"Kafka 会不会丢"两个面试题的共同根。

## 3. 存储机制：为什么 Kafka 能单机百万 TPS

### 3.1 磁盘布局

每个分区副本在磁盘上是一个目录 `topic-partition/`，里面是分段的日志文件组：

```
telecom-gnb-kpi-0/
├── 00000000000000000000.log          # 消息本体，按 offset 顺序追加
├── 00000000000000000000.index        # 稀疏索引：相对offset → 物理文件位置
├── 00000000000000000000.timeindex    # 时间索引：时间戳 → offset（按时间回溯用）
└── 00000000000001053600.log          # 新 Segment：到达 1GB 或 7 天后滚动
    └── ...（每个 Segment 一组 .log/.index/.timeindex）
```

设计精髓在三个"减法"：

1. **顺序写代替随机写**：追加写磁盘的顺序吞吐量可达数百 MB/s，接近内存随机写。传统 B 树存储引擎更新一页要"读页→改页→写回"，随机 I/O 在 HDD/SATA SSD 时代比顺序写慢 1~2 个数量级（实验二可实测：本机 NVMe 上读改写模式仍慢约 14 倍）；即便在随机写性能已很强的现代 NVMe 上，顺序写也让副本复制与故障恢复的 I/O 模式完全可预测。
2. **稀疏索引代替全量索引**：`.index` 不是每条消息一条索引，而是**每写入 index.interval.bytes（默认 4KB）数据加一条**。查找 offset=10536800 的消息：二分定位 Segment → 在 .index 里二分找到 ≤ 目标的最大索引项 → 顺序扫描少量消息。用少量内存索引覆盖 TB 级数据。
3. **页缓存代替进程内缓存**：Kafka **不自己管数据缓存**，全靠 OS 页缓存。写入先落页缓存就返回，读取命中页缓存时等效内存速度。好处：进程重启缓存不失效（OS 管）、避免 JVM 大堆 GC、32GB 机器能"用出"远超 JVM 堆的缓存。

### 3.2 零拷贝（sendfile）

传统发送路径要 4 次拷贝 + 4 次上下文切换：磁盘→页缓存→用户态缓冲→Socket 缓冲→网卡。Kafka 用 Linux `sendfile` 系统调用让数据**从页缓存直达 Socket 缓冲区**，用户态完全不经手。消费者拉取历史消息时这个优化收益最大（生产路径因为要校验/压缩通常仍经过用户态）。注意面试追问点：开启 SSL 加密后 sendfile 失效（数据必须经用户态加解密），这是"开启 TLS 后 Kafka 性能下降"的根因。

### 3.3 批处理与压缩

生产者把多条消息攒成**批（batch）**再发送，网络、磁盘、副本复制的开销都被摊薄。三个参数决定攒批行为：

| 参数 | 含义 | 调优直觉 |
|---|---|---|
| `linger.ms` | 批最长等待时间 | 从 0 调到 5~20ms，吞吐大涨、延迟小涨 |
| `batch.size` | 单批字节上限（默认 16KB） | 配合消息大小调到 64KB~1MB |
| `compression.type` | 压缩算法 | 生产主流 lz4/zstd；zstd 压缩比最高，lz4 CPU 开销最低。压缩在**生产者端**做，Broker 原样存，消费者解压——把 CPU 成本转移给了客户端 |

## 4. 副本机制与数据一致性（面试核心区）

### 4.1 Leader/Follower 与 ISR

每个分区一个 Leader 副本负责全部读写（Kafka 2.4 起支持 Follower 读但生产少用），其余为 Follower。Follower 唯一的工作是**不断向 Leader 发 Fetch 请求**复制数据。

**ISR（In-Sync Replicas）**是 Leader 维护的"跟得上进度"的副本名单，判定标准：**在 `replica.lag.time.max.ms`（默认 30s）内追上过 Leader 的日志末端（LEO）**。两种动态：

- **ISR 收缩**：Follower 网络抖动/GC 停顿/磁盘慢，超过阈值没追上 → 被踢出 ISR（Kafka 2.4 之前的 lag.bytes 数量标准已废弃，统一为时间标准，面试注意）。
- **ISR 扩展**：落后的副本追平 LEO → 重新进入 ISR。

```
LEO(Log End Offset)=10536800      Leader 日志末端
HW(High Watermark)=10536790       所有 ISR 副本都已复制的位置 ← 消费者最多读到 HW
Broker1(Leader): 0..10536800      ← LEO
Broker2(Follower): 0..10536798    ← 在 ISR（30s 内追过）
Broker3(Follower): 0..10536798    ← 在 ISR
HW = min(Broker2, Broker3 的 LEO) 与 Leader 取交集
```

**HW 与 LEO 的区别**是面试陷阱题：消费者**永远读不到 HW 之后的消息**——那部分数据 Leader 有了但 ISR 兜底还没齐，提前暴露会破坏"已读即已安全"的承诺。

### 4.2 acks 与 min.insync.replicas：不丢消息的生产端契约

| 配置组合 | 语义 | 风险 |
|---|---|---|
| `acks=0` | 发出去就算成功 | 网络闪断即丢，仅日志类可容忍 |
| `acks=1` | Leader 落页缓存即确认 | Leader 确认后立即宕机且未复制 → 丢；选主期间不可写 |
| `acks=all` | **ISR 内所有**副本确认 | 若 ISR 收缩到只剩 Leader 自己，退化成 acks=1 |

所以 `acks=all` 必须配 `min.insync.replicas≥2` + `replication.factor≥3`：ISR 最少要有 2 个副本在，任何单机故障都不丢失也不不可写。生产推荐三元组：**RF=3、min.isr=2、acks=all**，这是"消息不丢"的底线配置（三者缺一都是伪安全）。Broker 端对应配置 `unclean.leader.election.enable=false`：禁止 ISR 之外的副本当 Leader，宁可这个分区不可用也不丢数据——**CP 取向**；如果业务能容忍少量丢失换可用性（日志分析类），才允许 true（AP 取向）。通信网管场景通常选 CP：告警丢一条可能漏一次故障。

### 4.3 Leader 选举与 Controller

- 分区级：Leader 宕机时，Controller 从该分区 **ISR 中**选新 Leader（ISR 有序性保证新 Leader 拥有全部已提交数据）。
- 集群级：KRaft 下 Controller Leader 由 **Raft 多数派选举**产生，天然避免"脑裂双主"（Raft 任期制 + 多数派投票）。对比 ZK 模式：Controller 崩溃后所有 Broker 重新在 ZK 上抢临时节点，切换更慢且要全量同步元数据。

## 5. 消费模型与再平衡（一句话版，模块 16 展开）

消费者以 **Consumer Group** 为单位消费：组内每个分区只分给一个成员（单播分工），不同组各自全量消费（广播）。成员增减、分区数变化都会触发**再平衡（Rebalance）**，由 Group Coordinator（某个 Broker）主导，两阶段：JoinGroup（成员报到选 Leader）→ SyncGroup（分配方案下发）。**再平衡期间整个组停止消费（Stop The World）**，这是生产事故高发区——KIP-429 增量协作再平衡、KIP-848 新消费组协议（服务端分配，4.x 默认）都是为解决它而生。

## 6. 生产级调优速查表（详细实操在模块 17/20）

| 层 | 参数/手段 | 建议值/原则 |
|---|---|---|
| 生产者 | acks + retries + enable.idempotence | all / 整型最大值 / true（幂等防重） |
| Broker | num.io.threads / num.network.threads | 磁盘数×2 / CPU 核数附近起步 |
| Broker | log.retention / log.segment.bytes | 按容量与合规要求（电信审计常 7~30 天） |
| Topic | 分区数 | 目标吞吐 ÷ 单分区吞吐（单分区顺序写约 50~100MB/s） |
| 消费者 | max.poll.records / max.poll.interval.ms | 防止"处理太慢被踢出组→再平衡风暴" |
| JVM/OS | 堆 6GB 内 + 大页缓存 / vm.swappiness=1 | Kafka 不吃大堆，内存留给页缓存 |

## 7. 本模块小结（一段话复述检验）

Kafka 用"**分区做并行、副本做容灾、ISR 做一致性、顺序写+零拷贝+页缓存做吞吐**"四件套，把自己做成了通信级数据管道的事实标准；KRaft 用 Raft 共识把元数据从外部 ZooKeeper 收编进内部日志，让控制面与数据面同构。记住三个数字组合 RF=3/min.isr=2/acks=all，记住 LEO 与 HW 的关系，这个模块的面试题你已赢下一半。

---

## 延伸阅读（本项目内）

- 消息不丢失的三端完整分析：`bigdata/kafka/docs/interview-m15.md` 第 2 题
- KRaft 集群实操与元数据观察：`labs/lab-15-storage-and-isr.md`
- 消费者再平衡与事务：模块 16 文档
