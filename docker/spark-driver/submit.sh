#!/usr/bin/env bash
# =====================================================================
# 文件名: docker/spark-driver/submit.sh
# 说明: PySpark 作业提交包装：自动拼 Kafka 连接器 jar 与 Master 地址。
# 用法（容器内）: submit jobs/offline_attack_analysis.py
# =====================================================================
set -euo pipefail

MASTER="${SPARK_MASTER_URL:-spark://spark-master:7077}"
JARS="/opt/spark/work-dir/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar,/opt/spark/work-dir/jars/kafka-clients-3.4.1.jar,/opt/spark/work-dir/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar,/opt/spark/work-dir/jars/commons-pool2-2.12.1.jar"

exec spark-submit \
  --master "${MASTER}" \
  --deploy-mode client \
  --jars "${JARS}" \
  "$@"
