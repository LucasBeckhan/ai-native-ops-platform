# 模块 22/24：Spark SQL/DataFrame 与 Structured Streaming

> **配套代码**：`jobs/m22_alarm_analysis.py`、`m24_kpi_streaming.py`（均已实测）

---

## 1. Catalyst 优化器：为什么 DataFrame 比 RDD 快

```
Unresolved Logical Plan（只看语法）
  → Resolved（绑定 catalog：列存在吗？类型？）
  → Optimized Logical Plan（规则重写：谓词下推/列裁剪/常量折叠/广播 Join 决策）
  → Physical Plan（选算子实现：BroadcastHashJoin vs SortMergeJoin）
  → RDD 执行
```

**面试考点**：① RDD 黑盒 lambda，Catalyst 看不进去无法优化——DataFrame 带Schema，优化器可下推谓词、裁剪列；② 看 `explain(True)` 四层计划是排慢查询的第一步；③ Tungsten：堆外二进制存储+全阶段代码生成（消除虚函数开销）。

## 2. 窗口函数与关联分析实战（m22 已实测）

三个分析对应三种能力：
1. **Top 榜**（groupBy+orderBy）——热点设备识别（也是倾斜病灶确认）；
2. **每组 TopN**（`row_number().over(Window.partitionBy.orderBy)`）——每设备最近 3 条 critical 告警；
3. **跨表 MTTR**（广播 Join + unix_timestamp 差值 + 按厂商聚合）——运维月报经典："哪家厂商恢复最慢"。

**优化点清单**（代码注释同步标注）：维表广播避免 Shuffle、`unix_timestamp` 替代逐行构造成日期、SQL 与 DataFrame 双写对照（团队可维护性）。

## 3. Structured Streaming：把流当无限表

```
Kafka Source → 无限表不断追加 → 增量查询（每 trigger 批次）→ Sink
```

**三种输出模式**（m24 用 update）：

| 模式 | 输出内容 | 适用 |
|---|---|---|
| append | 只输出"确定不再变化"的行 | append-only 下游（有 watermark 才可用窗口） |
| update | 本次批次有变化的行 | 看"最新聚合值"（本项目） |
| complete | 全量结果 | 小状态的全量仪表 |

**微批 vs 持续处理**：微批（默认，trigger 间隔）延迟=批次间隔，吞吐优；`continuous`（实验性）毫秒级但算子受限。**与 Flink 对比**：Flink 纯流式事件驱动（毫秒级）、State 粒度细；Spark 微批简单可靠（复用批引擎），秒级延迟场景够用——按延迟预算选型而不是"谁高级"。

**Exactly-Once 的实现**：checkpointLocation 存 offset+状态 → 重跑从位点续 → Sink 幂等/事务（Kafka 事务 Sink / foreachBatch + 幂等写）。本项目实测：断点续跑不重算已提交批次。

## 4. 状态管理（streaming）

- **watermark**：同 Flink 语义（迟到容忍+状态清理触发）；
- **flatMapGroupsWithState**：任意自定义状态（会话化分析：用户上网会话切分）；
- **状态过大**：session 窗口高基数 key → 状态 TTL + RocksDB State Store（Spark 3.2+ 会话窗口原生支持）。

## 自检三问

1. Catalyst 四层计划？在哪层做广播 Join 决策？（1）
2. update 与 append 模式的区别与前提？（2/3）
3. 微批延迟的下界由什么决定？（3：trigger 间隔）
