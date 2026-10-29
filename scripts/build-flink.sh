#!/usr/bin/env bash
# =====================================================================
# 文件名: scripts/build-flink.sh
# 所属模块: 模块 18/19/20 公共构建入口
# 运行方式: ./scripts/build-flink.sh     （在项目根目录执行）
# 说明:
#   - 使用 maven:3.9.9-eclipse-temurin-17 容器构建（JDK 8 的旧版 javac
#     会内部崩溃，JDK 17 以 --release 8 语义编译后可在集群 Java 11/17 运行）。
#   - 挂载宿主 ~/.m2 缓存 + -o 离线模式：构建容器无需联网（绕开 daemon
#     代理问题）。首次构建请先在宿主机执行一次 `mvn -f bigdata/flink/pom.xml
#     package`（在线下载依赖到 ~/.m2），之后即可完全离线。
#   - 产物 bigdata/flink/target/teleops-flink-jobs.jar 已被 compose 挂载到
#     flink-jobmanager:/opt/flink/usrlib，无需重启容器即可提交。
# =====================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(dirname "${SCRIPT_DIR}")"

docker run --rm \
  -v "${REPO_ROOT}:/src" \
  -v "${HOME}/.m2:/root/.m2" \
  -w /src/bigdata/flink \
  maven:3.9.9-eclipse-temurin-17 \
  mvn -o -q package

echo "构建完成: bigdata/flink/target/teleops-flink-jobs.jar"
echo "提交示例: docker exec flink-jobmanager /opt/flink/bin/flink run -c com.teleops.flink.m19.CepIntrusionJob /opt/flink/usrlib/teleops-flink-jobs.jar"
