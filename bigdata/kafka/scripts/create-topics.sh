#!/usr/bin/env bash
# =====================================================================
# 文件名: bigdata/kafka/scripts/create-topics.sh
# 所属模块: 模块 15（也是全项目所有 Kafka 实验的公共入口）
# 运行方式:
#   ./create-topics.sh                 # 全部创建
#   ./create-topics.sh topic名         # 单个创建
# 说明:
#   - 生产环境必须关闭自动建 Topic（broker 端 auto.create.topics.enable=false，
#     本项目 compose 已设置），Topic 由脚本/流水线显式创建并纳入版本管理。
#   - RF=3 + min.insync.replicas=2 是"消息不丢"的底线配置（模块 15 文档 4.2）。
#   - telecom-gnb-kpi 按"基站分区"设计：key=基站ID 保证同基站时序在分区内有序。
# =====================================================================
set -euo pipefail

BOOTSTRAP="localhost:9092"  # 在 kafka-1 容器内执行，走容器内 PLAINTEXT 监听器（宿主机端口是 39092-39094）

# 统一的建 Topic 参数：三副本、min.isr=2、按时间保留 7 天
create() {
  local name="$1" partitions="$2"
  echo "== 创建 Topic: ${name} (partitions=${partitions}, RF=3, min.isr=2)"
  docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config retention.ms=604800000
}

ALL_TOPICS=(
  "telecom-gnb-kpi:6"        # 5G 基站 KPI（场景一/模块19/20 实时计算源）
  "telecom-gnb-alarms:3"     # 基站/核心网告警（模块16/22/28 数据源）
  "telecom-netflow:6"        # NetFlow 流量记录（场景二 DDoS 检测源）
  "telecom-firewall-events:3" # 防火墙事件（场景二 Agent 防御结果回写）
  "telecom-alerts:3"         # 平台内部告警事件（Flink 输出 → Agent 输入）
)

if [[ $# -ge 1 ]]; then
  for entry in "${ALL_TOPICS[@]}"; do
    if [[ "${entry%%:*}" == "$1" ]]; then
      create "$1" "${entry##*:}"
      exit 0
    fi
  done
  echo "未定义的 Topic: $1（可用列表见脚本内 ALL_TOPICS）" >&2
  exit 1
fi

for entry in "${ALL_TOPICS[@]}"; do
  create "${entry%%:*}" "${entry##*:}"
done

echo "== 全部 Topic 列表:"
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${BOOTSTRAP}" --list
