#!/usr/bin/env python3
# =====================================================================
# 文件名: bigdata/spark/jobs/m21_rdd_basics.py
# 所属模块: 模块 21 —— RDD 核心机制：血统、宽窄依赖、持久化
# 运行方式:
#   export SPARK_HOME=$(python3 -c "import pyspark,os;print(os.path.dirname(pyspark.__file__))")
#   $SPARK_HOME/bin/spark-submit --master "local[4]" bigdata/spark/jobs/m21_rdd_basics.py
# 说明:
#   - 用可观察输出的实验对照文档 21 节的四个概念：
#     ① 血统（Lineage）：转换不执行、行动才触发（惰性求值）；
#     ② 宽依赖 vs 窄依赖：toDebugString 里 Stage 划分可见；
#     ③ persist/cache：第二次 action 不重算血统（Job 数对比）；
#     ④ 分区：并行度与数据分布。
# =====================================================================
from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("M21-RDD-Basics").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("=" * 60)
    print("实验① 血统与惰性求值")
    # transformations 只是"记录血统"，不触发计算
    rdd = sc.parallelize(range(1, 1001), 4) \
            .map(lambda x: x * 2) \
            .filter(lambda x: x % 3 == 0)
    print("  两次 transformation 已定义但未执行（无 Job）")
    print("  action 触发:", rdd.take(3), "... 共", rdd.count(), "条")

    print("=" * 60)
    print("实验② 宽窄依赖（toDebugString 看 Stage 划分）")
    # 窄依赖：map/filter 一对一，同一 Stage
    narrow = sc.parallelize(range(100), 4).map(lambda x: x + 1)
    print("  窄依赖血统图:")
    print("  " + narrow.toDebugString().decode().strip().replace("\n", "\n  ")[:200])
    # 宽依赖：groupBy 产生 Shuffle → 切出新 Stage
    wide = narrow.groupBy(lambda x: x % 5)
    print("  加 groupBy（宽依赖/Shuffle）后:")
    print("  " + wide.toDebugString().decode().strip().replace("\n", "\n  ")[:300])
    print("  → ShuffleMapStage + ResultStage = 2 个 Stage（文档 21 节的 DAG 划分规则）")

    print("=" * 60)
    print("实验③ 持久化：cache 前后 Job 数对比")
    import time
    data = sc.parallelize(range(2_000_000), 8).map(lambda x: x ** 2 % 977)
    t0 = time.time(); print("  无缓存 count:", data.count(), f"耗时 {time.time()-t0:.2f}s")
    t0 = time.time(); print("  无缓存第二次 count:", data.count(), f"耗时 {time.time()-t0:.2f}s（血统重算）")
    data.cache()
    t0 = time.time(); print("  cache 后 count:", data.count(), f"耗时 {time.time()-t0:.2f}s（含物化）")
    t0 = time.time(); print("  cache 后第二次 count:", data.count(), f"耗时 {time.time()-t0:.2f}s（读缓存）")
    print("  → 第二次明显更快；生产注意 cache 的是'会被多次 action 的中间结果'")

    print("=" * 60)
    print("实验④ 分区数与并行度")
    print("  parallelize(range(1000), 4) 分区数:", sc.parallelize(range(1000), 4).getNumPartitions())
    print("  textFile 默认分区 = 文件块数；repartition(8) 增加并行、coalesce 减少不触发 Shuffle")
    print("=" * 60)
    spark.stop()


if __name__ == "__main__":
    main()
