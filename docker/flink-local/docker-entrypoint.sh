#!/bin/bash
# =====================================================================
# 文件名: docker/flink-local/docker-entrypoint.sh
# 所属模块: Flink 本地镜像入口脚本（等价官方镜像 entrypoint 的最小实现）
# 说明:
#   - 用法与官方镜像一致：entrypoint 接收 jobmanager 或 taskmanager。
#   - FLINK_PROPERTIES 环境变量的内容会被追加到 conf/flink-conf.yaml
#     （与官方镜像行为一致），compose 层的参数注入因此可用。
#   - 前台运行（exec），保证容器日志与进程信号透传。
# =====================================================================
set -e

ROLE="${1:-jobmanager}"

# 把 compose 注入的 FLINK_PROPERTIES 追加进配置文件（官方镜像同款机制）
if [ -n "${FLINK_PROPERTIES:-}" ]; then
  echo "${FLINK_PROPERTIES}" >> /opt/flink/conf/flink-conf.yaml
fi

case "${ROLE}" in
  jobmanager)
    exec /opt/flink/bin/jobmanager.sh start-foreground
    ;;
  taskmanager)
    exec /opt/flink/bin/taskmanager.sh start-foreground
    ;;
  *)
    echo "用法: entrypoint [jobmanager|taskmanager]，收到: ${ROLE}" >&2
    exit 1
    ;;
esac
