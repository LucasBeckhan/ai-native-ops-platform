#!/usr/bin/env bash
# =====================================================================
# 文件名: bigdata/kafka/scripts/lag_watch.sh
# 所属模块: 模块 17 —— Consumer Lag 连续观察工具（实验二/生产排障用）
# 运行方式:
#   ./lag_watch.sh <group>     # 每 3 秒刷新一次指定组的分区级 Lag
# 说明:
#   - 包装 kafka-consumer-groups --describe，做三件事：
#     ① 按分区对齐输出；② 计算组内 Lag 中位数与最大值；
#     ③ 自动给出"形态判定"提示（均匀上涨=消费端整体慢 / 独高=倾斜）。
#   - 该脚本在宿主机运行，通过 docker exec 进入容器调用 Kafka CLI。
#   - 不用 set -e：组不存在时 kafka CLI 报错属正常轮询场景，不能退出。
# =====================================================================

GROUP="${1:?用法: ./lag_watch.sh <group>}"
BRIDGE="docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092"

while true; do
  clear
  echo "===== $(date '+%H:%M:%S') Consumer Lag 观察 (${GROUP}) ====="
  # --describe 输出含表头与日志行，tail -n +2 去掉表头后每列位置固定：
  # $1=GROUP $2=TOPIC $4=PARTITION $6=CURRENT-OFFSET $8=LOG-END-OFFSET $10=LAG
  OUT="$(${BRIDGE} --describe --group "${GROUP}" 2>/dev/null | tail -n +2)"
  if [[ -z "${OUT}" ]]; then
    echo "(组 ${GROUP} 不存在或暂无分区数据；现有组列表：)"
    ${BRIDGE} --list 2>/dev/null
    sleep 3
    continue
  fi
  # --describe 输出列（Kafka 4.x）：
  # $1=GROUP $2=TOPIC $3=PARTITION $4=CURRENT-OFFSET $5=LOG-END-OFFSET $6=LAG
  # 只取 $3 为数字的行（过滤掉组元数据行）
  echo "${OUT}" | awk '$3 ~ /^[0-9]+$/ {printf "%-16s %-18s P%-3s cur=%-8s end=%-8s LAG=%s\n",$1,$2,$3,$4,$5,$6}'

  # 形态判定：LAG 列（$6）排序后取中位数与最大值对比
  echo "${OUT}" | awk '$6 ~ /^[0-9]+$/ {print $6}' | sort -n | awk '
    {a[NR]=$1}
    END{
      if (NR==0) {print "(无 Lag 数据)"; exit}
      mx = a[NR]; mx2 = (NR>=2) ? a[NR-1] : 0; mid = a[int((NR+1)/2)]
      if (mx >= 1000 && (mx >= mx2*5 || mx2 == 0))
        print ">> 判定：个别分区独高(" mx ") → 疑似数据倾斜（热点 key），加消费者实例无效"
      else if (mid >= 1000)
        print ">> 判定：各分区 Lag 均匀偏高 → 消费端整体变慢（查下游依赖/GC）"
      else if (mx > 0)
        print ">> 判定：轻微积压，持续观察"
      else
        print ">> 判定：无积压"
    }'
  sleep 3
done
