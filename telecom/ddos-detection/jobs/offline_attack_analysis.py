#!/usr/bin/env python3
# =====================================================================
# 文件名: telecom/ddos-detection/jobs/offline_attack_analysis.py
# 所属模块: 场景二 —— Spark 离线攻击模式分析（PySpark）
# 运行方式（spark-master 容器内执行，Kafka 连接器 jar 已预下载到 work-dir/jars）:
#   宿主机 local 模式（本实验环境验证通过）：
#     export SPARK_HOME=$(python3 -c "import pyspark,os;print(os.path.dirname(pyspark.__file__))")
#     export JAVA_HOME=<JDK8+>
#     KAFKA_BOOTSTRAP=localhost:39092 spark-submit --master local[4] \
#       --jars bigdata/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar,... \
#       telecom/ddos-detection/jobs/offline_attack_analysis.py
#   说明：Standalone 集群（spark://spark-master:7077）与 local 模式的取舍见
#   场景二文档 2.1 节——离线分析作业对学习者 local 模式即可完整演示；
#   集群模式的网络前提（driver/executors 与 Kafka 同网络）在文档中讲清。
# 说明:
#   - 离线层职责：消费 Kafka 的历史 NetFlow，产出"攻击画像"：
#     ① Top 源 IP（按纯 SYN 占比）② 攻击波次时间分布 ③ 特征总结。
#     实时层（Flink CEP）管"现在挡住谁"，离线层（Spark）管"过去谁打的、
#     什么模式"——两层各司其职（模块 25 的 Lambda 架构缩影）。
#   - 用 DataFrame/SQL 表达（模块 22 的实战应用），代码注释讲每个优化点。
# =====================================================================
import sys
from pyspark.sql import SparkSession, functions as F

import os
# 容器网络用服务名（kafka-1:9092），宿主机驱动用 localhost:39092
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP",
                                 "kafka-1:9092,kafka-2:9092,kafka-3:9092")


def main() -> None:
    spark = (SparkSession.builder
             .appName("TeleOps-DDoS-OfflineAnalysis")
             # 本地实验资源：4 worker 核内跑 2 executor；生产按资源公式规划（模块 23）
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())

    # ---- 读取 Kafka：批模式一次性读取全量历史（startingOffsets earliest）----
    raw = (spark.read
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", "telecom-netflow")
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load())

    # ---- 解析 JSON（get_json_object 内置函数，无 UDF 开销；模块 22 的优化点）----
    flows = (raw.selectExpr("CAST(value AS STRING) AS json_line")
             .select(
                 F.get_json_object("json_line", "$.src_ip").alias("src_ip"),
                 F.get_json_object("json_line", "$.dst_ip").alias("dst_ip"),
                 F.get_json_object("json_line", "$.dst_port").alias("dst_port"),
                 F.get_json_object("json_line", "$.flags").alias("flags"),
                 F.get_json_object("json_line", "$.packets").cast("long").alias("packets"),
                 F.get_json_object("json_line", "$.bytes").cast("long").alias("bytes"),
                 F.get_json_object("json_line", "$.ts").cast("long").alias("ts"),
             ))
    print(f"总流量记录数: {flows.count()}")

    # ---- 分析①：纯 SYN 占比 Top 源 IP（SYN Flood 的核心特征）----
    # 纯 SYN = flags 仅含 S（排除 SA/PA 等正常握手后续）
    syn_only = flows.where(F.col("flags") == "S")
    top_attackers = (flows.groupBy("src_ip")
                     .agg(F.count("*").alias("total_flows"),
                          F.sum(F.when(F.col("flags") == "S", 1).otherwise(0)).alias("syn_flows"),
                          F.avg("bytes").alias("avg_bytes"))
                     .withColumn("syn_ratio", F.round(F.col("syn_flows") / F.col("total_flows"), 3))
                     .where(F.col("total_flows") >= 5)
                     .orderBy(F.desc("syn_ratio"), F.desc("total_flows"))
                     .limit(10))
    print("\n== Top10 可疑源 IP（按纯 SYN 占比） ==")
    top_attackers.show(truncate=False)

    # ---- 分析②：攻击波次时间分布（按分钟聚合 SYN 包数）----
    waves = (syn_only
             .withColumn("minute", F.from_unixtime(F.col("ts") / 1000, "yyyy-MM-dd HH:mm"))
             .groupBy("minute", "src_ip")
             .agg(F.count("*").alias("syn_count"))
             .where(F.col("syn_count") >= 5)          # 与实时 CEP 阈值一致
             .orderBy("minute"))
    print("\n== 攻击波次（分钟 × 源IP，≥5 SYN） ==")
    waves.show(truncate=False)

    # ---- 分析③：攻击流与正常流的特征对比（给检测规则调参用的证据）----
    features = flows.withColumn("is_attack_like", F.col("flags") == "S")
    summary = features.groupBy("is_attack_like").agg(
        F.round(F.avg("bytes"), 1).alias("avg_bytes"),
        F.round(F.avg("packets"), 1).alias("avg_packets"),
        F.count("*").alias("flows"))
    print("\n== 攻击样态 vs 正常样态特征对比 ==")
    summary.show()

    spark.stop()


if __name__ == "__main__":
    main()
