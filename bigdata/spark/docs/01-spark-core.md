# 模块 21/23：Spark 架构原理与性能调优

> **前置依赖**：模块 15-17 | **预估学习时间**：8 小时 | **优先级**：P1
> **配套代码**：`bigdata/spark/jobs/m21_rdd_basics.py`（已实测）、`m23_skew_treatment.py`（已实测）

---

## 1. 架构：Driver、Executor 与调度体系

```
Driver（本作业的"大脑"）                    Executor（干活的进程，每 Worker 若干）
├── DAGScheduler：血统图 → Stage DAG        ├── TaskRunner：逐 Task 执行
│   宽依赖处切 Stage（Shuffle 边界）          ├── BlockManager：缓存/Shuffle 数据
├── TaskScheduler：Stage → TaskSet          └── 向 Driver 心跳汇报
│   按 locality 分发（PROCESS_LOCAL 最优）
└── SchedulerBackend：资源协商
```

**Stage 划分规则**（m21 实验②的 toDebugString 实证）：窄依赖（map/filter 一对一）合并进同一 Stage 流水线执行；**宽依赖（Shuffle：groupBy/reduceByKey/join）处切分**——因为必须等上游全部完成才能拉取数据。

## 2. RDD 四特征与血统

| 特征 | 一句话 | 本项目实验 |
|---|---|---|
| 分区列表 | 并行度单元 | 实验④ getNumPartitions |
| 依赖列表 | 血统=重算路径 | 实验①②（惰性求值+Shuffle 切分） |
| 计算函数 | 每分区怎么算 | map/filter 链 |
| 分区器（可选） | Key→分区（Hash/Range） | 决定 Shuffle 后的分布 |

**血统容错的本质**：不备份数据而记录"怎么重算"；Checkpoint 才是物理落盘（血统过长/依赖外部数据时用）。

## 3. 内存模型与 GC

```
Executor JVM 堆:
├── Reserved (300MB)
├── Unified Memory (spark.memory.fraction=0.6，可互借)
│   ├── Execution（Shuffle/Join/Sort）  ← spill 到磁盘
│   └── Storage（cache）               ← 可被 Execution 挤占（evict）
└── User Memory（用户数据结构/UDF 对象）
```

**GC 调优三步**：① 看 GC 时间占比（Web UI >15% 异常）；② cache 的 RDD 对象多 → 换 `MEMORY_ONLY_SER`（Kryo 序列化，省 2-4 倍）；③ 老年代频繁 Full GC → 加堆或减 Storage 占用（`spark.memory.storageFraction` 降）。**因果别搞反**：先查倾斜/反压再动内存，多数"内存问题"根子是数据分布。

## 4. Shuffle 深入（面试必考）

- **Write/Read 两阶段**：Map 端每 Task 按 Reduce 分区数写临时文件（SortShuffle：先按分区排序+溢写合并成一个数据文件+索引文件）；Reduce 端拉取归并；
- **为什么 SortShuffle 取代 Hash Shuffle**：文件数从 `M×R` 降到 `M`（+索引），万级 Task 不再文件句柄爆炸；
- **bypass 机制**：分区数 ≤ `spark.shuffle.sort.bypassMergeThreshold`(200) 且无 map 端聚合时，免排序直接写文件（更快）；
- **调优杠杆**：`spark.sql.shuffle.partitions`（200 默认对小作业太大）、AQE 自动合并小分区（Spark 3.x 必开）、压缩（lz4/zstd）。

## 5. 数据倾斜：五种解法与选择树（m23 实测）

```
确认倾斜（病灶确认：key 分布 + Web UI 单 task 耗时/读写量独高）
   │
   ├─ 倾斜发生在 Join 且一侧是小表 ──► ① 广播 Join（F.broadcast / AQE 自动）★最优先
   ├─ 倾斜发生在聚合 ───────────────► ② 两阶段聚合（加盐预聚合+合并）★m23 实测
   ├─ 热点 key 业务可拆（大客户）────► ③ 单独拆出单独算再 union
   ├─ Join 双方都大且个别 key 倾斜 ──► ④ 加盐 + 膨胀匹配（倾斜侧加 N 倍盐、
   │                                    另一侧复制 N 份，join 后去重）
   └─ 短期兜底 ────────────────────► ⑤ AQE skewJoin 自动拆分（3.x）
```

m23 实测（7000 行小数据）：直接 1.08s / 加盐 1.16s / 两阶段 0.53s——小数据差异不大，**生产亿级+热点 80% 时差异是数量级的**（文档如实话术，面试要能解释"为什么小数据测不出"）。

## 6. 资源规划公式

```
executor 数 = 总核数 / spark.executor.cores（cores 4~5 个，GC 与 HDFS 客户端并发最优）
executor 内存 = spark.executor.memory + overhead(0.1×，最小 384MB)
分区数 ≈ 总核数 × 2~3（CPU 打满且有流水余量）
动态分配（spark.dynamicAllocation.enabled）+ 外部 Shuffle Service：批作业省钱标配
```

## 7. Spark on K8s vs YARN（选型）

| | YARN | K8s（Spark Operator） |
|---|---|---|
| 生态 | 大数据全家桶原生 | 云原生栈（本项目选型） |
| 弹性 | NodeLabel/队列 | Namespace/Quota/HPA 联动 |
| 运维 | 独立 Hadoop 集群 | 与业务同底座 |
| 适合 | 存量 Hadoop 体系 | 云原生新建（通信行业云化趋势） |

## 自检四问

1. Stage 在哪里切分？为什么？（1：宽依赖）
2. Storage 和 Execution 内存能互借吗？谁挤占谁？（3：Execution 可驱逐 Storage）
3. 五种倾斜解法的选择树？（5）
4. 为什么 SortShuffle 是 M 个文件而非 M×R？（4）
