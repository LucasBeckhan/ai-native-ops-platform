#!/usr/bin/env python3
# =====================================================================
# 文件名: bigdata/kafka/src/consumer_idempotent.py
# 所属模块: 模块 16 —— 幂等消费：用"业务主键 + 处理结果表"消化重复消息
# 运行方式:
#   python3 consumer_idempotent.py                # 正常消费
#   python3 consumer_idempotent.py --rewind 20    # 故意回退 20 条重放（制造重复）
# 说明:
#   - 为什么需要幂等消费：生产端 at-least-once（重试）+ 消费端
#     "处理完才提交位移"（防丢）=> 两者叠加必然出现重复消费。
#     Kafka 事务只解决 read-process-write 闭环，不解决"处理动作本身"
#     的副作用幂等，所以最终防线在业务侧。
#   - 方案：本地 SQLite 维护"已处理主键表"，处理与登记在同一本地事务里，
#     重复消息直接跳过。生产中该表通常由数据库唯一索引/Redis SETNX 承担。
#   - --rewind 模拟位移回退（生产中对应：消费者组位移被误重置、
#     处理后崩溃未提交等场景），验证去重逻辑真的兜得住。
# =====================================================================
import argparse
import json
import signal
import sqlite3
import sys

from confluent_kafka import Consumer, TopicPartition as TP

BOOTSTRAP = "localhost:39092,localhost:39093,localhost:39094"
TOPIC = "telecom-gnb-alarms"
GROUP = "idempotent-demo-group"
DB_PATH = "/tmp/consumer_idempotent.db"


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    # 业务主键 = seq + device_id（由生产端 syslog_producer.py 生成）
    conn.execute(
        "CREATE TABLE IF NOT EXISTS processed ("
        " dedup_key TEXT PRIMARY KEY,"      # 唯一索引：数据库层兜底防重
        " processed_at REAL NOT NULL)"
    )
    conn.commit()
    return conn


def handle_alarm(conn: sqlite3.Connection, alarm: dict) -> bool:
    """处理一条告警；返回 True=新数据，False=重复跳过。
    生产实现中"处理"与"登记"应在同一个业务事务里（此处 SQLite 事务演示）。"""
    key = f"{alarm['seq']}:{alarm['device_id']}"
    cur = conn.execute("SELECT 1 FROM processed WHERE dedup_key=?", (key,))
    if cur.fetchone():
        return False
    # ---- 真实业务动作占位区：例如写入网管工单库、触发通知 ----
    conn.execute(
        "INSERT INTO processed(dedup_key, processed_at) VALUES(?, ?)",
        (key, __import__("time").time()),
    )
    conn.commit()
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="幂等消费实验")
    parser.add_argument("--rewind", type=int, default=0, help="启动时把位移回退 N 条制造重复")
    args = parser.parse_args()
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))

    conn = init_db()
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,        # 本实验聚焦去重，自动提交即可
        "enable.partition.eof": True,
    })
    if args.rewind:
        # 取当前组位移并回退 N 条：模拟"位移被误重置"
        parts = consumer.committed([TP(TOPIC, p) for p in range(3)], timeout=10)
        rewound = [TP(p.topic, p.partition, max(0, (p.offset or 0) - args.rewind))
                   for p in parts if p.offset is not None and p.offset > 0]
        if rewound:
            consumer.assign(rewound)
            print(f"已回退位移 {args.rewind} 条: {[(t.topic, t.partition, t.offset) for t in rewound]}")
    else:
        consumer.subscribe([TOPIC])

    fresh, dup, total = 0, 0, 0
    idle_rounds = 0          # 连续空轮计数：超过上限视为"数据读完了"，避免无限等待
    try:
        while total < 300 and idle_rounds < 5:
            msgs = consumer.consume(num_messages=50, timeout=2.0)
            if not msgs or all(m.error() for m in msgs):
                idle_rounds += 1
                continue
            idle_rounds = 0
            for m in msgs:
                if m.error():
                    continue
                total += 1
                if handle_alarm(conn, json.loads(m.value())):
                    fresh += 1
                else:
                    dup += 1
    finally:
        consumer.close()
        conn.close()
    print(f"共消费 {total} 条：新处理 {fresh} 条，识别重复 {dup} 条并跳过。")
    print("结论：位移回退/重试带来的重复被'业务主键唯一表'完全消化 => 幂等消费生效。")


if __name__ == "__main__":
    main()
