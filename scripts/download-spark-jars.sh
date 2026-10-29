#!/usr/bin/env bash
# =====================================================================
# 文件名: scripts/download-spark-jars.sh
# 所属模块: 场景二 / 模块 24 —— Spark Kafka 连接器离线准备
# 运行方式: ./scripts/download-spark-jars.sh    （在项目根目录执行）
# 说明:
#   - Structured Streaming / 批读 Kafka 需要 4 个 jar。宿主机下载（走可用
#     网络）到 bigdata/spark/jars/，该目录已挂载进 spark 容器 work-dir/jars，
#     spark-submit 用 --jars 引用——离线环境不依赖提交时联网下载。
#   - 版本与镜像 apache/spark:v3.5.5 对齐。
# =====================================================================
set -euo pipefail

JARS_DIR="bigdata/spark/jars"
mkdir -p "${JARS_DIR}"

BASE="https://repo1.maven.org/maven2"
# 国内网络可把 BASE 换成 https://maven.aliyun.com/repository/central

JARS=(
  "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar"
  "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"
  "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.5/spark-token-provider-kafka-0-10_2.12-3.5.5.jar"
  "org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar"
)

for path in "${JARS[@]}"; do
  fname="${path##*/}"
  if [[ -f "${JARS_DIR}/${fname}" ]]; then
    echo "已存在: ${fname}"
    continue
  fi
  echo "下载: ${fname}"
  curl -fsSL --retry 3 -o "${JARS_DIR}/${fname}" "${BASE}/${path}"
done

ls -lh "${JARS_DIR}"
echo "提交命令示例："
echo "  docker exec spark-master spark-submit --jars jars/spark-sql-kafka-0-10_2.12-3.5.5.jar,jars/kafka-clients-3.4.1.jar,jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar,jars/commons-pool2-2.12.1.jar jobs/offline_attack_analysis.py"
