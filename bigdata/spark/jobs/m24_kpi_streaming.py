#!/usr/bin/env python3
# =====================================================================
# 文件名: bigdata/spark/jobs/m24_kpi_streaming.py
# 所属模块: 模块 24 —— Structured Streaming：Kafka 实时 KPI 聚合与异常检测
# 运行方式（需 Kafka 有数据，两个终端）:
#   终端1: python3 telecom/5g-slice-monitor/generator/kpi_generator.py --scenario degrade
#   终端2: $SPARK_HOME/bin/spark-submit --master "local[4]" \
#            --jars bigdata/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar,... \
#            bigdata/spark/jobs/m24_kpi_streaming.py
# 说明:
#   - 微批（默认）处理：trigger=10s 一批；与 Flink 的持续处理对比是
#     模块 24 面试核心（文档 24 节的对比表）。
#   - 事件时间 30s 窗口 + 5s watermark + 迟到数据容许；
#     checkpointLocation 提供断点续跑（Exactly-Once 的文件/Kafka Sink 前提）。
#   - 输出 mode=update：只输出窗口内有变化的结果（与 complete/append 的差异见文档）。
# =====================================================================
import os
import shutil
from pyspark.sql import SparkSession, functions as F

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP",
                                 "kafka-1:9092,kafka-2:9092,kafka-3:9092")
# 容器网络内无法写宿主机路径，统一用 /tmp（实验用途）；
# 生产写 HDFS/对象存储（Exactly-Once 语义依赖可靠存储）
CKPT = "/tmp/spark-kpi-ckpt"


def main() -> None:
    spark = (SparkSession.builder.appName("M24-KPI-Streaming")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    shutil.rmtree(CKPT, ignore_errors=True)   # 实验每次从头跑；生产严禁删除！

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", "telecom-gnb-kpi")
           .option("startingOffsets", "latest")
           .load())

    # 解析 + 事件时间：ts 字段转 timestamp，成为窗口的时间轴
    parsed = (raw.selectExpr("CAST(value AS STRING) AS line")
              .select(F.get_json_object("line", "$.sliceId").alias("slice_id"),
                      F.get_json_object("line", "$.latency_ms").cast("double").alias("latency_ms"),
                      F.from_unixtime(F.get_json_object("line", "$.ts") / 1000).cast("timestamp")
                       .alias("event_time"))
              .where(F.col("slice_id").isNotNull()))

    # 30s 滚动窗口 + 5s watermark：迟到 5s 内的数据仍计入窗口
    agg = (parsed
           .withWatermark("event_time", "5 seconds")
           .groupBy(F.window("event_time", "30 seconds"), F.col("slice_id"))
           .agg(F.count("*").alias("samples"),
                F.round(F.avg("latency_ms"), 1).alias("avg_latency"),
                F.max("latency_ms").alias("max_latency"))
           .where(F.col("avg_latency") > 10)      # uRLLC SLA 阈值判定（演示用统一阈值）
           .select("slice_id", "avg_latency", "max_latency", "samples"))

    query = (agg.writeStream
             .outputMode("update")                 # 有变化的窗口才输出
             .format("console")
             .option("truncate", "false")
             .option("checkpointLocation", CKPT)   # 断点续跑/Exactly-Once 前提
             .trigger(processingTime="10 seconds") # 微批间隔（模块 24 核心参数）
             .start())

    print("Streaming 查询已启动（10s 微批），观察上方窗口输出；Ctrl+C 停止")
    query.awaitTermination()


if __name__ == "__main__":
    main()
