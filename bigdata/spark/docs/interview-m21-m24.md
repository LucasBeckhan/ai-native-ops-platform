# 模块 21-24 面试题：Spark 核心（10 道）

> 每道题：**面试官在考察什么** → **标准答案** → **加分回答** → **常见误区**

---

## Q1. RDD 的宽窄依赖区别？Stage 怎么划分？

**考察**：调度地基。**标准**：窄依赖=父分区一对一（map/filter/union），可流水线执行；宽依赖=父分区被多子分区依赖（groupBy/reduceByKey/join 非广播），需 Shuffle。DAGScheduler 在宽依赖处切 Stage：窄链合并为一个 Stage 流水线，Shuffle 处切出 ShuffleMapStage + ResultStage。**加分**：join 有例外——co-partitioned RDD join 是窄依赖（各自已按同 key 分区），这是"预分区避免 Shuffle"的原理；toDebugString 直接看血统（m21 实验）。**误区**：以为所有 join 都是宽依赖；把 Stage 划分说成"按算子类型"。

## Q2. Spark 的 Shuffle 原理与调优？

**考察**：性能问题的第一大来源。**标准**：SortShuffle——Map 端按分区排序溢写合并成单数据文件+索引，Reduce 端拉取；文件数 M（对比 Hash 的 M×R）。bypass：分区数≤200 且无 map 端聚合免排序。调优：shuffle.partitions 适配数据量、AQE 合并小分区、压缩（zstd）、`repartition` vs `coalesce`（后者合并分区不触发 Shuffle）。**加分**：External Shuffle Service 的作用（Executor 释放后 Reduce 仍可拉取，动态分配前提）；push-based shuffle（Magnet，3.2+）；Spill 判定看 Execution 内存块。**误区**：盲目调 partitions=200；把 spill 当错误（是保护机制）。

## Q3. 数据倾斜五种解法与选择树？

**考察**：生产最常遇到。**标准**：见文档 5 节选择树——①广播 Join（小表侧）②两阶段聚合③热点业务拆分④加盐膨胀匹配⑤AQE 自动倾斜 Join。**加分**：m23 实测口径（小数据测不出差异的原因：shuffle 分区 8 个、单 task 也够快——必须用生产量级+80% 热点复现）；倾斜与 OOM 的关系（单 task 数据超 executor 分配 → spill 或 OOM）；Web UI 确认方法。**误区**：上 salt 就完事（去重类不可加盐）；广播超大表（driver/executor OOM）。

## Q4. Catalyst 优化器流程？举一个优化例子？

**考察**：SQL 引擎理解。**标准**：四层——解析→分析（绑定 catalog）→逻辑优化（规则：谓词下推/列裁剪/常量折叠）→物理计划（Join 策略选择）→代码生成。例子：`SELECT a FROM t WHERE day='2026-10-29'` 读 Parquet 时列裁剪只读 a 列、谓词下推跳过不含该日期的 RowGroup。**加分**：explain(True) 看四层；AQE 的三个能力（小分区合并/倾斜 Join 拆分/Join 策略动态切换）是 3.x 面试新考点；CBO 统计信息驱动。**误区**：UDF 是黑盒阻断下推（尽量用内置函数）；不知道 Tungsten 全阶段代码生成。

## Q5. Spark 内存模型与 GC 调优？

**考察**：稳定性运维。**标准**：统一内存（execution+storage 互借，storage 可被 execution 驱逐）、User Memory、Overhead（堆外 0.1×）。GC 调优：看 GC 时间占比>15% 异常 → 序列化 cache（Kryo）→ 调 storageFraction → 大堆换 G1。**加分**：`spark.memory.fraction=0.6` 与 unified 模型的历史（对比 1.6 前的静态分配）；off-heap 的取舍；"先查倾斜再动内存"的排障顺序。**误区**：OOM 就加堆（倾斜时加多少都崩）；把 overhead 当可省项。

## Q6. cache/persist 的级别怎么选？什么时候 cache 反而有害？

**考察**：工程细节。**标准**：MEMORY_ONLY（默认）→ MEMORY_AND_DISK（放不下落盘）→ *_SER（Kryo 序列化省内存）→ DISK_ONLY。cache 有害的场景：RDD 只被 action 一次（白占内存+物化开销）、cache 超大数据集挤掉更常用的块、流式作业无界累积。**加分**：unpersist 的时机（用完即释）；checkpoint 与 cache 的区别（血统截断 vs 缓存，血缘重算 vs 物理副本）；m21 实验的 Job 数视角。**误区**：所有 RDD 都 cache；忘记 unpersist。

## Q7. Spark on K8s 与 YARN 的区别？迁移要注意什么？

**考察**：平台选型（通信行业云化背景）。**标准**：资源模型（Pod vs Container）、调度（K8s scheduler vs YARN）、存储访问、镜像化依赖打包（Spark 镜像 vs PYTHON 环境分发）。迁移注意：动态分配需 K8s 的 external shuffle service 替代方案（driver 端 shuffle 或 PodGC 策略）、HDFS→对象存储、本地路径→PVC。**加分**：Spark Operator（K8s CRD）声明式提交作业；"镜像=环境"解决 Python 依赖分发痛点；多租户 quota。**误区**：以为 K8s 模式没有外部 shuffle；忽略 executor Pod 启动延迟对微批作业的影响。

## Q8. Structured Streaming 的 Exactly-Once 怎么实现？

**考察**：流语义精确性。**标准**：checkpointLocation 持久化 offset+状态 → 每 batch 处理完原子更新位点 → Sink 幂等（Kafka 事务/foreachBatch 幂等写/文件提交协议）。恢复后从最近位点重放未提交批次 → 靠 Sink 幂等消重。**加分**：与 Flink 对比（同为 checkpoint 机制，Spark 是微批粒度天然对齐批次与位点）；端到端 test：kill -9 后重启从 checkpoint 续跑（m24 可验证）。**误区**：以为 checkpoint 就够了（Sink 不幂等照样重复）；checkpoint 目录放本地盘（作业迁移即失效）。

## Q9. 微批 vs 持续处理的选型？与 Flink 怎么比？

**考察**：流引擎架构视野。**标准**：微批延迟=trigger 间隔（100ms~分钟），吞吐高、复用批引擎、运维简单；Flink 纯流事件驱动毫秒级、CEP/状态原生；延迟预算 >1s 选 Spark 微批完全够（KPI 秒级告警 ✓），要求亚秒且复杂事件处理选 Flink。**加分**：continuous 模式的实验性限制；本项目场景一二正是"延迟敏感用 Flink CEP、离线画像用 Spark"的分工实证；两类引擎的团队技能栈成本。

**误区**：唯延迟论；忽略团队栈与运维成本。

## Q10. 给"亿级告警日志关联分析"做资源规划与作业设计？

**考察**：综合设计（对应 m22 的放大版）。**标准**：① 存储列式化（Parquet+分区表 day/region）；② Join 优化：维表 <100MB 广播，否则排序合并+倾斜处理；③ 资源：核数=数据量/单核吞吐（经验 50MB/s）、分区=核×2~3、AQE 开；④ 增量化：按天分区只算增量，维表 SCD 缓变处理；⑤ 产出物化到 Parquet 供 BI 直查。**加分**：告警关联的时序窗口 Join 设计（foreach 窗口/RANGE BETWEEN）；数据质量前置（脏数据过滤算子入 DAG）；成本账（spot 节点+动态分配）。

**误区**：小文件问题（写 Parquet 后 compact）；把关联分析写成笛卡尔积级联。
