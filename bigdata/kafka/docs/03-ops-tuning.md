# 模块 17：Kafka 集群运维与调优

> **前置依赖**：模块 15/16
> **预估学习时间**：5 小时（文档 1.5h + 实验 2.5h + 面试题 1h）
> **配套实验**：`labs/lab-17-ops.md`（Broker 故障演练/分区重分配/Lag 排查/指标开启）

---

## 1. 集群规划：从业务量反推资源

### 1.1 三步规划法

```
第1步 算流量：峰值消息速率 × 平均消息大小 = 峰值带宽（入）
        例：10万基站×20条/s×500B = 1GB/s（入），下游3个消费组×1 = 3GB/s（出）
第2步 算副本：入带宽 × 副本因子 = 集群总写入
        例：1GB/s × 3 = 3GB/s 的集群级磁盘写入
第3步 算节点：总写入 ÷ 单节点安全写入水位（磁盘顺序写的 30%，留峰值余量）
        例：3GB/s ÷ (500MB/s×0.3×2盘) ≈ 10 节点
```

**分区数**：`max(入带宽/单分区写入, 出带宽/单分区消费) × 1.5 冗余`，并保证是消费端并行度（Flink 并行度）的整数倍。**副本因子**：日志类 2 可接受，网管告警类必须 3 + min.isr=2。

### 1.2 Broker 资源模型（经验公式）

| 资源 | 公式/建议 | 依据 |
|---|---|---|
| CPU 核数 | 网络线程 num.network.threads≈3~8，IO 线程 num.io.threads≈磁盘数×2 | 压缩/解压与 SSL 是 CPU 大头 |
| 内存 | JVM 堆 6GB 封顶，其余全给页缓存 | 页缓存命中率决定读性能 |
| 磁盘 | 每天数据量 × 保留天数 × 副本因子 × 1.3 | 留 compaction 与水位余量 |
| 网卡 | 入带宽 × 副本因子 × 1.2 | 副本复制占大头，常被忽略 |

## 2. 监控告警：五个指标定生死

| 指标 | 含义 | 告警阈值经验 | 高危根因 |
|---|---|---|---|
| **UnderReplicatedPartitions (URP)** | 复制未跟上 Leader 的分区数 | >0 即告警（持续 5 分钟） | Follower 掉队（GC/磁盘/网络）、副本不足 |
| **Consumer Lag** | 消费组落后 Leader 的消息数 | 按组基线，突发 >5×基线 | 消费端处理变慢/再平衡风暴/分区倾斜 |
| **RequestLatency p99** | Produce/Fetch 请求 p99 延迟 | Produce >100ms、Fetch >500ms | 磁盘 await 上升、ISR 收缩、网络 |
| **ActiveControllerCount** | 存活 Controller 数 | 集群内必须恒 =1 | 双 Controller（脑裂征兆，立即处置） |
| **OfflinePartitionsCount** | 无 Leader 分区数 | >0 为 P0 事故 | 多副本同时故障 / unclean 选举被禁 |

**Consumer Lag 的正确打开方式**（面试高频）：
```
kafka-consumer-groups.sh --describe --group <g>
  → 看 LAG 列 + CONSUMER-ID 列：
    ① Lag 全分区均匀上涨    → 消费端整体变慢（下游 DB 慢？GC？）
    ② 个别分区 Lag 独高     → 数据倾斜（某基站告警量爆炸）或该分区消费慢
    ③ CONSUMER-ID 为空      → 分区没人消费（成员死了没触发再平衡/组挂了）
```
Prometheus 侧：Kafka Exporter（拉集群指标）或 JMX Exporter（Broker 进程内指标），Grafana 大盘的必备面板见 `docker/monitoring`（本项目已配好抓取 job，见 prometheus.yml 中 kafka-jmx 注释）。

## 3. 故障排查：三类高频故障的标准动作

### 3.1 Broker 宕机（模拟演练见 lab-17 实验一）

```
现象：某 Broker 进程退出
动作序列：
1. kafka-topics --describe  → 观察 Leader 迁移与 ISR 变化（Leader 会自动切到 ISR 内副本）
2. 确认 URP 是否清零、业务写入是否恢复（min.isr 满足则写入不中断）
3. 修 Broker → 重启 → 追日志（追平 LEO）→ 重新入 ISR
4. 演练复盘：记录 Leader 迁移耗时、写入恢复耗时、追平耗时
```

### 3.2 分区倾斜（某分区流量独高）

根因多为 key 设计：大客户/热点基站 key 哈希扎堆。解法：**key 加盐**（`key = 业务key + "#" + 随机后缀`，下游聚合时二次聚合去后缀）或复合 key 细化。**注意**：Kafka 分区数不可减少，扩分区会让"同 key 分区映射"变化，出现跨分区乱序窗口——扩分区要在下游能容忍乱序时做（见 3.3）。

### 3.3 消费者再平衡风暴

```
排查序列：
1. 看 Broker 日志 PrepareRebalance 频率 → 确认风暴
2. 看成员离开原因：session timeout（心跳断）vs max.poll.timeout（处理太慢）
3. 处理太慢 → max.poll.records 调小 / 处理逻辑异步化 / 扩消费者（≤分区数）
4. session 超时 → 查成员 GC 日志与网络
5. 若发布频繁 → 优雅关闭（close 触发 leave）+ 滚动发布间隔
```

### 3.4 磁盘故障

单盘故障：Broker 下线该盘（`log.dirs` 移除该目录需迁移副本，JBOD 场景）或整机下线修复。**副本是唯一救命稻草**：RF=3 + 机架感知（broker.rack）保证同分区副本不落同机架——机房级故障也不至于全灭。这是"为什么 RF 不能是 2"的终极答案：RF=2 时单机故障后只剩 1 副本，min.isr=2 直接拒绝写入，等于单点。

## 4. 调优清单（按收益排序）

| 层 | 手段 | 收益 |
|---|---|---|
| 生产者 | 攒批+压缩（linger 20ms + lz4/zstd） | 请求量降 5~10 倍 |
| Broker | 关闭 auto.create.topics；分区数规划到位 | 防失控 Topic；避免热分区 |
| Broker | `log.retention` 分级（热 7 天 + 归档到对象存储） | 磁盘成本降 60%+ |
| 主题 | `compression.type=producer`（透传生产端压缩） | Broker 零压缩 CPU |
| 消费者 | fetch.min.bytes+fetch.max.wait.ms 攒批拉取 | 网络往返降一个量级 |
| OS | vm.swappiness=1、文件句柄 100k+、XFS | 基础设施级稳定 |
| JVM | Kafka 堆 6GB 内 + G1，别给大堆 | 页缓存才是主战场 |

**KRaft 特有运维点**（新考点）：① Controller 节点奇数部署，`quorum.voters` 变更需滚动；② 元数据日志目录与数据目录分开监控（`__cluster_metadata` 满 = 全集群瘫痪）；③ 滚动升级顺序：Controller 先于 Broker；④ `kafka-metadata-quorum.sh describe` 观察 Leader/Lag（lab-15 已用过）。

## 5. 容量演练与演练记录表模板

每次演练记录四列：**故障注入方式 / MTTR（检测→恢复）/ 业务影响（丢否、延迟峰值）/ 暴露的盲区**。演练价值不在"能恢复"，而在暴露监控盲区与预案缺陷——lab-17 会让你填出第一张真实记录表。
