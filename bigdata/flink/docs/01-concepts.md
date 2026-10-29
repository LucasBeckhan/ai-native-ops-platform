# 模块 18：Flink 架构与核心概念

> **前置依赖**：模块 15（Kafka，作为数据源背景）
> **预估学习时间**：6 小时（文档 2h + 实验 3h + 面试题 1h）
> **配套代码**：`bigdata/flink/src/main/java/com/teleops/flink/m18/`（两个作业，已在真集群验证）
> **配套实验**：`labs/lab-18.md`

---

## 1. 集群架构：JobManager、TaskManager 与 Slot

```
        ┌── JobManager 容器（本作业集群 1 个）──────────────┐
        │  Dispatcher   ← REST 提交入口（8081 Web UI）       │
        │  JobMaster    ← 单个作业的调度与 Checkpoint 协调    │
        │  ResourceManager ← Slot 资源池管理                 │
        └──────────────┬───────────────────────────────────┘
                       │ 申请/释放 Slot
        ┌── TaskManager ×2（本 compose 配置）────────────────┐
        │  Slot 0: [Source] [Window] [Sink] 的并行子任务      │
        │  Slot 1: 同上另一并行度                             │
        └───────────────────────────────────────────────────┘
```

三个易混概念一次说清（面试高频）：
- **TaskManager**：进程（JVM），持有资源；
- **Slot**：TM 内的资源分片（本配置每 TM 2 Slot），决定"最多能同时跑几个并行子任务"；
- **Task/SubTask**：算子链的执行实例。**Operator Chain** 把上下游并行度一致且本地转发的算子合并成一个 Task（减少线程切换与序列化）——`1s窗口聚合` 作业在 Web UI 的 JobGraph 里能看到链合并效果。

**部署模式对比**（一句话版）：会话集群（本项目用，先起集群再提作业，实验友好）/ Per-Job（每作业独立集群，已废弃方向）/ **Application 模式**（作业+main 一起在集群执行，生产主流）。

## 2. 时间语义：Event Time 是流处理的"宪法"

| 语义 | 含义 | 用途 |
|---|---|---|
| Event Time | 事件**发生**时刻（数据自带 ts 字段） | 正确性：乱序/延迟下结果确定 |
| Processing Time | 系统处理时刻 | 低延迟但不精确（重启/背压会扭曲） |
| Ingestion Time | 进入 Flink 时刻 | 少用 |

通信场景的必然选择是 Event Time：基站 KPI 的 `ts` 字段是设备侧产生的，网络传输抖动不应改变"这一秒的平均吞吐"归属到哪个窗口。

### 2.1 Watermark：乱序容忍的进度声明

```
事件流（乱序）:  t=100, t=104, t=101, t=110, t=98（迟到）
Watermark = 已见最大事件时间 - 允许乱序度(200ms)
            → watermark 到达 110-200=负值起步，随最大值推进单调上涨

窗口 [t, t+1s) 触发条件: watermark ≥ 窗口 end - 1ms
迟到数据: 事件时间 < 当前 watermark → 默认丢弃（可旁路收集）
```

三个必须内化的推论：
1. Watermark 是**单调的**，取多分区/多通道的**最小值**对齐——**一个空闲的输入分区会把全局 watermark 冻结在初始值**（本项目实测踩到的真实坑：Kafka 六分区只有两个有数据，作业"看起来正常"但 CEP 永远不输出。解法 `withIdleness`，见 lab-18）；
2. 乱序度参数是**正确性与延迟的交换**：容忍 5 秒乱序 = 结果最多晚 5 秒出；
3. `timesOrMore` 这类**开放量词**的 CEP 模式要等 watermark 关闭 `within` 窗口才输出匹配（lab-19 实证）。

## 3. 窗口机制：三种窗口与触发器

| 窗口 | 语义 | 通信场景示例 |
|---|---|---|
| Tumbling（滚动） | 固定长度不重叠 | 每分钟 KPI 统计（本项目 KafkaKpiAlertJob） |
| Sliding（滑动） | 长度+步长，可重叠 | 每 10 秒输出最近 1 分钟速率 |
| Session（会话） | 活动间隙 ≥gap 则切分 | 用户会话级信令分析 |

**增量聚合 vs 全量聚合**（面试陷阱）：纯 `ProcessWindowFunction` 把窗口内**所有元素攒在状态里**再处理（内存差）；正确姿势是 `AggregateFunction`（增量，状态只有累加器）+ `ProcessWindowFunction`（只拿窗口元数据）组合——本项目 m18/m19 作业均按此写法并注释了理由。

## 4. 状态管理：Keyed State 与状态后端

```
KeyedProcessFunction 里的 ValueState<Long>：
  ├── 按 key 隔离（每个基站一份计数）
  ├── Checkpoint 自动快照（与位点原子绑定）
  └── TTL 可配（防止 key 无限增长——通信场景必须配，基站会退服）
状态后端选型:
  HashMapStateBackend   —— 状态在 JVM 堆，小状态低延迟（本项目实验默认）
  EmbeddedRocksDB       —— 状态落磁盘，TB 级状态/大窗口（生产大作业标配）
```

## 5. Checkpoint：Chandy-Lamport 分布式快照的工程实现

```
Checkpoint 流程（每 10s 一次，本作业实测完成耗时 12~55ms）:
1. JobMaster 的 CheckpointCoordinator 向 Source 注入 Barrier（特殊标记）
2. Barrier 随数据流向下游；算子收到全部输入通道的 Barrier 后：
   a. 把自身状态快照写入 StateBackend（异步，不阻塞处理）
   b. 向 JobMaster 确认
3. Source 同时把"当前消费位点"（Kafka offset）并入快照
   → 状态与位点原子对齐 = Exactly-Once 恢复的基础
```

**恢复语义的实验实证**（lab-18 实验二）：作业在第 50 条注入崩溃 → 3 秒后自动重启 → 状态从最近 Checkpoint 恢复 → 输入从位点重放 → **状态计数"接着走"，但部分事件被重放**（at-least-once 输入 + 恢复后的重处理）。这正是"端到端 Exactly-Once 需要 Sink 侧事务/幂等"的原因——恢复机制保证的是"状态不丢不重"，端到端还差最后一公里（模块 20 文档 4.3）。

## 自检四问

1. 为什么空闲分区会冻结全局 watermark？怎么解？（2.1 推论 1）
2. 纯 ProcessWindowFunction 有什么内存问题？（3）
3. Checkpoint 里除了算子状态还存了什么？（5：Kafka 位点）
4. 恢复后为什么会有重复输入？靠什么补齐端到端？（5）
