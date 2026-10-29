#!/usr/bin/env python3
# =====================================================================
# 文件名: telecom/base-station-ops/ml/train_failure_model.py
# 所属模块: 场景三 —— 预测性维护：设备故障预测模型训练（Spark MLlib）
# 运行方式:
#   /tmp/sparkenv/bin/spark-submit --master "local[4]" \
#     telecom/base-station-ops/ml/train_failure_model.py
# 说明:
#   - 场景三"预测性维护"的离线层：基于设备时序特征训练故障分类模型。
#   - 数据合成：无历史数据时合成可复现的训练集（温度/电压/重启次数/
#     告警数 → 30 天内是否故障）；生产替换为真实的动环+工单 join 数据集。
#   - 用 Spark MLlib Pipeline（VectorAssembler + RandomForest）——
#     生产主流结构（特征工程/训练/评估/导出同管道，可版本化）。
#   - 输出评估指标（AUC/准确率）与特征重要度——"哪个信号最能预示故障"
#     是运维最有价值的产出。
# =====================================================================
import random

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

random.seed(7)


def synth_data(spark, n: int = 20000):
    """合成训练集：特征与标签的因果链设计（温度高+电压低+重启多 → 故障）。
    生产中该表由 Spark 批作业从 Kafka 明细聚合而成（30 天窗口特征）。"""
    rows = []
    for _ in range(n):
        avg_temp = random.gauss(35, 6)                # 平均温度 ℃
        temp_std = abs(random.gauss(3, 2))            # 温度波动
        min_voltage = random.gauss(53, 1.5)           # 最低电压 V
        restarts_30d = random.choices([0, 1, 2, 3], [70, 20, 7, 3])[0]
        alarms_30d = random.choices([0, 1, 2, 5, 10], [50, 20, 15, 10, 5])[0]
        age_years = random.uniform(0.5, 8)
        # 标签生成（注入真实因果）：多因素叠加
        risk = (max(0, avg_temp - 38) * 0.5 + max(0, 52 - min_voltage) * 1.2 +
                restarts_30d * 0.8 + alarms_30d * 0.15 + age_years * 0.3)
        label = 1 if random.random() < 1 / (1 + pow(2.718, -(risk - 4))) else 0
        rows.append((avg_temp, temp_std, min_voltage, restarts_30d,
                     alarms_30d, age_years, label))
    cols = ["avg_temp", "temp_std", "min_voltage", "restarts_30d",
            "alarms_30d", "age_years", "label"]
    return spark.createDataFrame(rows, cols)


def main() -> None:
    spark = SparkSession.builder.appName("M-Scenario3-FailureModel").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    data = synth_data(spark)
    train, test = data.randomSplit([0.8, 0.2], seed=42)

    features = ["avg_temp", "temp_std", "min_voltage", "restarts_30d",
                "alarms_30d", "age_years"]
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    rf = RandomForestClassifier(labelCol="label", featuresCol="features",
                                numTrees=50, maxDepth=6, seed=42)

    pipeline = Pipeline(stages=[assembler, rf])
    model = pipeline.fit(train)

    # ---- 评估：AUC 与准确率（预测性维护的核心指标）----
    pred = model.transform(test)
    auc = BinaryClassificationEvaluator(labelCol="label",
                                        metricName="areaUnderROC").evaluate(pred)
    acc = pred.where(F.col("label") == F.col("prediction")).count() / test.count()
    print(f"测试集 AUC: {auc:.3f}")
    print(f"测试集 准确率: {acc:.3f}")

    # ---- 特征重要度："哪个信号最能预示故障" ----
    rf_model = model.stages[-1]
    print("\n特征重要度（从高到低）:")
    importances = sorted(zip(features, rf_model.featureImportances.toArray()),
                         key=lambda x: -x[1])
    for name, imp in importances:
        print(f"  {name:14} {imp:.3f}")

    # ---- 导出：保存 Pipeline 模型供 Flink/在线服务加载（场景三的在线推理）----
    model.write().overwrite().save("/tmp/teleops-failure-model")
    print("\n模型已导出: /tmp/teleops-failure-model（生产存 HDFS/对象存储 + 版本化）")
    spark.stop()


if __name__ == "__main__":
    main()
