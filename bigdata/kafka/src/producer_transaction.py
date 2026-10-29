#!/usr/bin/env python3
# =====================================================================
# 文件名: bigdata/kafka/src/producer_transaction.py
# 所属模块: 模块 16 —— 事务生产者：多分区原子写 + 消费-转换-生产闭环
# 运行方式:
#   python3 producer_transaction.py                     # 正常事务提交
#   python3 producer_transaction.py --abort-at 5        # 第 5 批故意中止（回滚验证）
# 说明:
#   - Kafka 事务保证：**跨多个分区/Topic 的多批消息要么全部可见、要么全部不可见**
#     （原子性）。典型场景：消费-转换-生产（consume-transform-produce）闭环，
#     把"消费位移提交"与"下游生产"放进同一事务，实现端到端 Exactly-Once。
#   - 事务三步曲：initTransactions（注册 PID）→ beginTransaction →
#     send_offsets_to_transaction(消费位移) / produce(多条) → commit_transaction。
#   - 消费端必须配 isolation.level=read_committed 才看不见未提交消息——
#     否则回滚的"脏消息"仍会被读到（lab-16 实验四验证）。
# =====================================================================
import argparse
import json
import time

from confluent_kafka import Consumer, Producer, TopicPartition

BOOTSTRAP = "localhost:39092,localhost:39093,localhost:39094"
SRC_TOPIC = "telecom-gnb-alarms"
DST_TOPIC = "telecom-alerts"
GROUP = "txn-demo-group"  # 可用 --group 覆盖；被强杀过消费者后建议换新组（旧组要等 max.poll.interval 超时才清场）

BATCHES = 8
BATCH_SIZE = 50


def main() -> None:
    parser = argparse.ArgumentParser(description="事务生产者实验")
    parser.add_argument("--abort-at", type=int, default=0, help="在第 N 批中止事务（0=不中止）")
    parser.add_argument("--group", default=GROUP, help="消费组 ID")
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        # 必填且跨重启全局唯一：Broker 靠它找回未完成事务并做隔离 fencing
        "transactional.id": "teleops-txn-producer-01",
        "acks": "all",
    })
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": args.group,
        "auto.offset.reset": "earliest",
        # 只读已提交消息；配错成 read_uncommitted 会读到回滚前的脏数据
        "isolation.level": "read_committed",
        # 闭环中位移随事务提交，必须关闭消费端自动提交
        "enable.auto.commit": False,
    })
    consumer.subscribe([SRC_TOPIC])

    # 第 1 步：初始化事务。若上次有未完成事务（如中途崩溃），Broker 会按
    # transactional.id 找回状态并回滚未完成部分——这就是"崩溃恢复"。
    producer.init_transactions(30)
    print("事务已初始化（transactional.id=teleops-txn-producer-01）")

    processed_batches = 0
    empty_rounds = 0   # 连续空轮计数：源数据读尽则退出（生产中应改为持续运行）
    try:
        while processed_batches < BATCHES and empty_rounds < 6:
            msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=3.0)
            if not msgs:
                empty_rounds += 1
                continue
            empty_rounds = 0

            # 2a. 聚合本批消费位移：每分区取最大 offset+1（下次从这继续）
            next_offsets: dict[tuple[str, int], int] = {}
            for m in msgs:
                key = (m.topic(), m.partition())
                next_offsets[key] = max(next_offsets.get(key, 0), m.offset() + 1)

            producer.begin_transaction()

            # 2b. "转换"：严重告警升级为平台告警事件写入下游 Topic
            produced = 0
            for m in msgs:
                alarm = json.loads(m.value())
                if alarm.get("severity") in ("critical", "major"):
                    event = {
                        "type": "alarm",
                        "device_id": alarm["device_id"],
                        "derived_at": int(time.time() * 1000),
                        "origin_seq": alarm.get("seq"),
                        "summary": alarm["message"],
                    }
                    producer.produce(
                        DST_TOPIC, key=m.key(), value=json.dumps(event).encode()
                    )
                    produced += 1

            # 2c. 消费位移并入同一事务（聚合后一次提交，避免同分区多次提交）
            offsets = [
                TopicPartition(topic, partition, offset)
                for (topic, partition), offset in next_offsets.items()
            ]
            producer.send_offsets_to_transaction(
                offsets, consumer.consumer_group_metadata(), 30
            )

            # 3. 提交或中止：成功则"位移+下游消息"原子生效；中止则全部回滚
            if args.abort_at and processed_batches + 1 == args.abort_at:
                producer.abort_transaction(30)
                print(f"第 {processed_batches + 1} 批：事务已中止"
                      f"（{len(msgs)} 条消费位移与 {produced} 条下游消息全部回滚）")
            else:
                producer.commit_transaction(30)
                print(f"第 {processed_batches + 1} 批：事务已提交"
                      f"（{len(msgs)} 条入，{produced} 条下游产出）")
            processed_batches += 1
    finally:
        consumer.close()
    print(f"完成 {processed_batches} 批处理。下游 {DST_TOPIC} 中只会出现已提交批次的数据。")


if __name__ == "__main__":
    main()
