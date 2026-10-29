#!/usr/bin/env python3
# =====================================================================
# 文件名: bigdata/kafka/src/producer_idempotent.py
# 所属模块: 模块 16 —— 幂等生产者与"重试乱序"实验
# 运行方式:
#   python3 producer_idempotent.py --mode idempotent   # 幂等模式（推荐）
#   python3 producer_idempotent.py --mode naive        # 无幂等模式（对比实验）
# 说明:
#   - 幂等生产者解决两个问题：① 网络重试导致的重复写入；② 开启重试后
#     max.in.flight>1 导致的"后发先至"乱序。
#   - 实现原理（面试高频）：Broker 为每个 <PID, 分区> 维护一个序号，
#     Producer 给每批消息带序号，Broker 发现序号回退/重复就拒绝——
#     重复与乱序在 Broker 端被精确识别。
#   - 实验设计：注入"首批发送失败再重试"的场景，观察两种模式下
#     消费端读到的顺序差异（详见 lab-16 实验三）。
# =====================================================================
import argparse
import json

from confluent_kafka import Producer

BOOTSTRAP = "localhost:39092,localhost:39093,localhost:39094"
TOPIC = "telecom-alerts"


def make_producer(mode: str) -> Producer:
    if mode == "idempotent":
        conf = {
            "bootstrap.servers": BOOTSTRAP,
            "enable.idempotence": True,     # 开幂等：PID+序号机制
            "acks": "all",                  # 幂等模式下强制 all（即使配 1 也会被覆盖为 all）
            "retries": 10,
            # 幂等模式下 in-flight 上限 5（Broker 端 5 窗口去重，>5 会被拒绝）
            "max.in.flight.requests.per.connection": 5,
        }
    else:
        conf = {
            "bootstrap.servers": BOOTSTRAP,
            "enable.idempotence": False,    # 关闭幂等：重试可能重复
            "acks": "all",
            "retries": 10,
            # >1 且无幂等：一旦首批重试，就可能出现乱序（实验核心观察点）
            "max.in.flight.requests.per.connection": 5,
        }
    return Producer(conf)


def main() -> None:
    parser = argparse.ArgumentParser(description="幂等生产者对比实验")
    parser.add_argument("--mode", choices=["idempotent", "naive"], default="idempotent")
    parser.add_argument("--run-id", default="run1", help="实验轮次标识，用于消费端区分")
    args = parser.parse_args()

    producer = make_producer(args.mode)
    # 发送 50 条带严格序号的消息到同一分区（固定 key）
    for i in range(50):
        payload = {"run": args.run_id, "mode": args.mode, "seq": i}
        producer.produce(
            TOPIC,
            key=b"order-test",   # 固定 key → 单分区 → 才有"顺序"可言
            value=json.dumps(payload).encode(),
        )
    producer.flush(30)
    print(f"[{args.mode}] 已发送 run={args.run_id} 的 50 条 seq=0..49 到分区(key=order-test)")
    print("消费端验证命令见 lab-16 实验三：检查 seq 是否严格递增、有无重复。")


if __name__ == "__main__":
    main()
