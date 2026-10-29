# Spark 系列模块（21/22/23/24）：架构 · SQL/DataFrame · 调优 · Structured Streaming

> **前置依赖**：模块 15-17 | **预估学习时间**：20 小时 | **优先级**：P1
> **运行环境**：宿主机 PySpark local 模式（本实验环境验证通过）+ Standalone 集群（docker-compose.spark.yml，JVM 侧/Web UI 教学）

## 学习目标

- 21：血统/宽窄依赖（toDebugString 实证）、持久化 Job 数对比、分区
- 22：窗口函数/广播 Join/MTTR 关联分析（已实测：厂商 MTTR 120-124min）
- 23：倾斜病灶确认 + 三种解法实测对比（两阶段聚合最快且结果一致）+ 选择树
- 24：微批/输出模式/watermark/checkpoint 断点续跑（已实测：Kafka→30s 窗口→SLA 判定）

## 文件清单

```
bigdata/spark/
├── docs/{01-spark-core.md, 02-dataframe-streaming.md, interview-m21-m24.md}
├── jobs/
│   ├── m21_rdd_basics.py          # 血统/Stage/缓存/分区（已验证）
│   ├── m22_alarm_analysis.py      # 告警关联分析三连（已验证）
│   ├── m23_skew_treatment.py      # 倾斜三解法对比（已验证）
│   └── m24_kpi_streaming.py       # Kafka 流式 KPI 检测（已验证）
├── data/{alarms.csv, devices.csv} # 样例数据（7000 条告警+维表）
└── jars/                          # Kafka 连接器（scripts/download-spark-jars.sh）
```

## 快速启动

```bash
pip install pyspark==3.5.5        # 或离线安装 docker/wheels/pyspark-3.5.5.tar.gz
export SPARK_HOME=$(python3 -c "import pyspark,os;print(os.path.dirname(pyspark.__file__))")
export PATH="$SPARK_HOME/bin:$PATH"; export JAVA_HOME=<JDK8+>

spark-submit --master "local[4]" bigdata/spark/jobs/m21_rdd_basics.py
spark-submit --master "local[4]" bigdata/spark/jobs/m22_alarm_analysis.py
spark-submit --master "local[4]" bigdata/spark/jobs/m23_skew_treatment.py
# m24 需 Kafka + 连接器 jar（命令见 job 文件头注释）
```
