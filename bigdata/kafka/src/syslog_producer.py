#!/usr/bin/env python3
# =====================================================================
# 文件名: bigdata/kafka/src/syslog_producer.py
# 所属模块: 模块 16 —— 通信场景实战：设备日志实时采集管道（Syslog/SNMP Trap → Kafka）
# 运行方式:
#   python3 syslog_producer.py                 # 持续生产，Ctrl+C 停止
#   python3 syslog_producer.py --count 200     # 只发 200 条后退出（实验用）
# 说明:
#   - 模拟网管采集器（如 syslog-ng/SNMP Trap 接收机）：把设备上报的
#     Syslog 行与 SNMP Trap 转成结构化 JSON 写入 telecom-gnb-alarms。
#   - 设计要点①：key=设备ID（eNB-xxxx）。同一设备的告警必然"产生/清除"
#     成对出现，必须落在同一分区保序，否则网管状态机会错乱（文档 3.1 铁律2）。
#   - 设计要点②： acks=all + 幂等 + 重试 = 模块 15 文档 4.2 的底线配置。
#   - 设计要点③：用 delivery 回调统计失败，失败消息落本地文件重放——
#     生产采集器的标准容灾姿态（上游短暂不可用时不能丢设备数据）。
# =====================================================================
import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP = "localhost:39092,localhost:39093,localhost:39094"   # 宿主机外部监听器
TOPIC = "telecom-gnb-alarms"

# 网管系统常见的告警字典（真实告警语义，让实验数据贴近生产）
ALARM_LIBRARY = [
    {"code": "ALM-0101", "text": "小区退服", "severity": "critical"},
    {"code": "ALM-0102", "text": "光模块接收异常", "severity": "major"},
    {"code": "ALM-0203", "text": "CPU 利用率超阈值", "severity": "major"},
    {"code": "ALM-0305", "text": "X2 链路中断", "severity": "critical"},
    {"code": "ALM-0402", "text": "温度过高预警", "severity": "minor"},
    {"code": "ALM-0403", "text": "温度恢复正常", "severity": "cleared"},
]
FAILED_LOG = "/tmp/syslog_producer_failed.log"

running = True


def _stop(sig, _frame):
    """Ctrl+C 优雅退出：让 flush 把在途消息发完。"""
    global running
    running = False


def delivery_callback(err, msg):
    """发送结果回调：失败消息写本地文件，支持事后重放（采集器容灾姿态）。"""
    if err is not None:
        with open(FAILED_LOG, "a", encoding="utf-8") as f:
            f.write(msg.value().decode("utf-8") + "\n")
        print(f"[delivery失败] {err}，已写入 {FAILED_LOG}", file=sys.stderr)


def build_alarm(seq: int) -> dict:
    """合成一条结构化告警事件。字段设计对齐网管北向接口（Syslog/Trap 语义）。"""
    lib = random.choice(ALARM_LIBRARY)
    device = f"eNB-{random.randint(1001, 1050):04d}"
    return {
        "seq": seq,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source_protocol": random.choice(["syslog", "snmp-trap"]),
        "device_id": device,
        "alarm_code": lib["code"],
        "severity": lib["severity"],
        "message": f"{device} {lib['text']}",
        "cell_id": f"cell-{random.randint(1, 3)}",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="模拟网管采集器")
    parser.add_argument("--count", type=int, default=0, help="发送条数，0=持续发送")
    parser.add_argument("--interval", type=float, default=0.05, help="发送间隔秒")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _stop)

    producer = Producer({
        # 文档 4.2 的"不丢"三元组：acks=all 必须配幂等（自动开启重试去重）
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 2147483647,
        # 攒批：20ms 或 64KB 触发发送，吞吐优先的采集器参数
        "linger.ms": 20,
        "batch.size": 65536,
        "compression.type": "lz4",
    })

    sent, seq = 0, 0
    while running:
        alarm = build_alarm(seq)
        # key=设备ID 字节串 → 保证同设备消息固定分区（保序）
        producer.produce(
            TOPIC,
            key=alarm["device_id"].encode(),
            value=json.dumps(alarm, ensure_ascii=False).encode(),
            on_delivery=delivery_callback,
        )
        # 队列满时 poll 会阻塞等待，防止无限占用内存（采集器背压）
        producer.poll(0)
        seq += 1
        sent += 1
        if args.count and sent >= args.count:
            break
        time.sleep(args.interval)

    producer.flush(30)   # 退出前清空在途队列，否则最后一批会丢
    print(f"已发送 {sent} 条告警到 {TOPIC}（key=设备ID，分区保序）")


if __name__ == "__main__":
    main()
