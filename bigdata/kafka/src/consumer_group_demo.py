#!/usr/bin/env python3
# =====================================================================
# 文件名: bigdata/kafka/src/consumer_group_demo.py
# 所属模块: 模块 16 —— 消费组、手动位移提交与再平衡监听
# 运行方式:
#   python3 consumer_group_demo.py --group demo-a --member m1   # 终端1
#   python3 consumer_group_demo.py --group demo-a --member m2   # 终端2（触发再平衡）
#   Ctrl+C 退出任一成员（再次触发再平衡）
# 说明:
#   - 三个高频生产配置演示：手动位移提交（处理完再提交）、
#     精细再平衡回调（revoke 时先提交/清理）、位移提交策略。
#   - librdkafka 默认是 cooperative-sticky（增量协作再平衡），
#     这也是 KIP-429 之后的推荐策略：再平衡只迁移被撤销的分区，
#     其余成员继续消费（对比 Java 客户端旧默认 range/roundrobin 的
#     "全组 Stop-The-World"，面试常考）。
#   - 手动提交的三条铁律：① 处理完再提交（防丢）；② 提交"最大已处理
#     offset+1"（语义：下一条从这开始）；③ revoke 回调里必须先提交
#     被撤销分区的位移，否则再平衡期间已处理未提交的部分会被重消费。
# =====================================================================
import argparse
import json
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError
from confluent_kafka import TopicPartition as TP

BOOTSTRAP = "localhost:39092,localhost:39093,localhost:39094"
TOPIC = "telecom-gnb-alarms"

running = True


def _stop(sig, _frame):
    global running
    running = False


def on_assign(consumer: Consumer, partitions: list[TP]) -> None:
    """分区分配完成回调：打印拿到的分区，本例从最早位点开始。"""
    for p in partitions:
        p.offset = TP.OFFSET_BEGINNING   # 实验需要全量回放；生产常用 OFFSET_STORED
    consumer.assign(partitions)
    print(f"[assign] {sys.argv} 拿到分区: {[(p.topic, p.partition) for p in partitions]}")


def on_revoke(consumer: Consumer, partitions: list[TP]) -> None:
    """分区撤销回调（增量协作再平衡只撤销被迁移的分区）。
    关键动作：同步提交当前已处理位移，防止重复消费。"""
    consumer.commit(asynchronous=False)
    print(f"[revoke] 撤销分区并同步提交位移: {[(p.topic, p.partition) for p in partitions]}")


def on_lost(consumer: Consumer, partitions: list[TP]) -> None:
    """分区丢失回调（会话超时等被强抢）：不需要提交（位移已被新 owner 接管），
    只做本地状态清理。这个回调是排查'再平衡风暴'时的关键观察点。"""
    print(f"[lost] 强制失去分区: {[(p.topic, p.partition) for p in partitions]}")


def main() -> None:
    parser = argparse.ArgumentParser(description="消费组与再平衡实验")
    parser.add_argument("--group", default="demo-group")
    parser.add_argument("--member", default="m1", help="成员名，多开几个进程对比分区分配")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, _stop)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": args.group,
        "client.id": f"{args.group}-{args.member}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,      # 手动提交：处理完才提交（防丢的第一步）
        # librdkafka 的默认协议即 incremental cooperative（增量协作）
        "partition.assignment.strategy": "cooperative-sticky",
    })
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)

    # 本地缓存每个 (topic,partition) 已处理到的最大 offset，revoke 时统一提交
    pending_max_offset: dict[tuple[str, int], int] = {}
    processed = 0

    while running:
        msgs = consumer.consume(num_messages=20, timeout=1.0)
        for m in msgs:
            if m.error():
                if m.error().code() == KafkaError._PARTITION_EOF:
                    continue   # 追到末尾属正常，实验环境开了 enable.partition.eof
                print(f"[消费异常] {m.error()}", file=sys.stderr)
                continue
            alarm = json.loads(m.value())
            # ---- 业务处理区（这里用打印模拟）----
            processed += 1
            if processed % 50 == 0:
                print(f"[{args.member}] 已处理 {processed} 条，最近: {alarm['device_id']} {alarm['message']}")
            pending_max_offset[(m.topic(), m.partition())] = m.offset() + 1

        # 批量异步提交：以最大已处理 offset+1 语义提交（处理完才提交）
        if pending_max_offset:
            consumer.commit(
                offsets=[TP(t, p, o) for (t, p), o in pending_max_offset.items()],
                asynchronous=True,
            )
            pending_max_offset.clear()

    # 优雅关闭：同步提交残留位移，再 leave group（避免触发一次多余的再平衡等待）
    consumer.commit(asynchronous=False)
    consumer.close()
    print(f"[{args.member}] 退出，共处理 {processed} 条")


if __name__ == "__main__":
    main()
