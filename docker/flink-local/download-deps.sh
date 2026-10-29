#!/usr/bin/env bash
# =====================================================================
# 文件名: docker/flink-local/download-deps.sh
# 所属模块: Flink 本地镜像构建的前置步骤
# 运行方式: ./download-deps.sh   （在 docker/flink-local/ 目录执行）
# 说明:
#   - 下载 Flink Kafka 连接器 jar 到 ../dist（构建上下文）。
#   - 国内网络优先清华/阿里镜像源，失败回退 Maven Central。
# =====================================================================
set -euo pipefail

DIST_DIR="../dist"
mkdir -p "${DIST_DIR}"
JAR="flink-sql-connector-kafka-3.0.2-1.18.jar"
GROUP_PATH="org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18"

if [[ -f "${DIST_DIR}/${JAR}" ]]; then
  echo "连接器 jar 已存在: ${DIST_DIR}/${JAR}"
  exit 0
fi

URLS=(
  "https://mirrors.tuna.tsinghua.edu.cn/apache/flink/flink-1.18.1/${JAR}"
  "https://repo1.maven.org/maven2/${GROUP_PATH}/${JAR}"
  "https://repo.maven.apache.org/maven2/${GROUP_PATH}/${JAR}"
)
# 说明：flink-sql-connector-kafka 不随 Flink 发行包分发，清华 apache 镜像
# 不承载 Maven 构件，因此实际上 Maven Central 是主要来源；保留多源回退逻辑。

for url in "${URLS[@]}"; do
  echo "== 尝试: ${url}"
  if curl -fsSL --max-time 300 -o "${DIST_DIR}/${JAR}" "${url}"; then
    echo "== 下载成功: ${DIST_DIR}/${JAR}"
    ls -lh "${DIST_DIR}/${JAR}"
    exit 0
  fi
done
echo "全部下载源失败，请检查网络或手动下载 ${JAR} 放入 ${DIST_DIR}" >&2
exit 1
