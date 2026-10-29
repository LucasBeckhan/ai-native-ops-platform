#!/usr/bin/env python3
# =====================================================================
# 文件名: bigdata/spark/jobs/m22_alarm_analysis.py
# 所属模块: 模块 22 —— DataFrame/SQL：窗口函数、多源 Join、告警关联分析
# 运行方式:
#   $SPARK_HOME/bin/spark-submit --master "local[4]" bigdata/spark/jobs/m22_alarm_analysis.py
# 说明:
#   - 通信场景实战（对应全栈文档 22 节）："网络设备告警日志关联分析"——
#     告警流水 ⨝ 设备维表，三个典型分析：
#     ① 设备维度告警 Top 榜（热点设备，倾斜源识别）
#     ② 窗口函数：每设备最近 3 条 critical 告警（row_number over 分区窗口）
#     ③ MTTR 平均恢复时长（cleared_at - raised_at，按厂商聚合——
#        "哪家厂商的故障恢复慢"是运维月报的经典问题）
#   - 优化点在注释里：维表广播 Join、字符串转时间的成本、谓词下推。
# =====================================================================
from pyspark.sql import SparkSession, functions as F, Window


def main() -> None:
    spark = (SparkSession.builder.appName("M22-Alarm-Analysis")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    base = "bigdata/spark/data"
    alarms = (spark.read.csv(f"{base}/alarms.csv", header=True, inferSchema=True))
    devices = (spark.read.csv(f"{base}/devices.csv", header=True, inferSchema=True))
    alarms.printSchema()

    # ---- 分析①：告警 Top 榜（也是模块 23 倾斜实验的"病灶确认"）----
    print("== 分析① 设备告警 Top5（热点设备） ==")
    (alarms.groupBy("device_id").agg(F.count("*").alias("alarm_count"),
                                     F.sum(F.when(F.col("severity") == "critical", 1).otherwise(0)).alias("critical_n"))
     .orderBy(F.desc("alarm_count")).limit(5).show())

    # ---- 分析②：每设备最近 3 条 critical 告警（窗口函数）----
    print("== 分析② 每设备最近 3 条 critical 告警（row_number over Window） ==")
    w = Window.partitionBy("device_id").orderBy(F.desc("raised_at"))
    latest_critical = (alarms.where(F.col("severity") == "critical")
                       .withColumn("rn", F.row_number().over(w))
                       .where(F.col("rn") <= 3)
                       .select("device_id", "alarm_id", "raised_at"))
    latest_critical.show(6, truncate=False)

    # ---- 分析③：MTTR（平均恢复时长）按厂商聚合——多源 Join 实战 ----
    print("== 分析③ MTTR 按厂商聚合（告警 ⨝ 设备维表，广播 Join） ==")
    # 优化点：维表 <10MB 走广播，避免 Shuffle 大表（F.broadcast）
    joined = alarms.join(F.broadcast(devices), "device_id", "inner")
    # 优化点：时间差用 to_unix_timestamp，避免逐条构造成日期对象
    mttr = (joined.withColumn("mttr_minutes",
                              (F.unix_timestamp("cleared_at") - F.unix_timestamp("raised_at")) / 60)
            .groupBy("vendor")
            .agg(F.round(F.avg("mttr_minutes"), 1).alias("avg_mttr_min"),
                 F.count("*").alias("alarms"),
                 F.round(F.avg(F.when(F.col("severity") == "critical", 1).otherwise(0)) * 100, 1)
                  .alias("critical_pct"))
            .orderBy(F.desc("avg_mttr_min")))
    mttr.show()

    # SQL 等价写法（团队里 SQL 工程师可维护，注册临时视图）
    alarms.createOrReplaceTempView("alarms")
    devices.createOrReplaceTempView("devices")
    print("== SQL 等价实现（Region 维度 MTTR） ==")
    spark.sql("""
        SELECT region, ROUND(AVG((unix_timestamp(cleared_at)-unix_timestamp(raised_at))/60),1) AS mttr_min,
               COUNT(*) AS alarms
        FROM alarms GROUP BY region ORDER BY mttr_min DESC
    """).show()

    spark.stop()


if __name__ == "__main__":
    main()
