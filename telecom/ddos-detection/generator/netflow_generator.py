#!/usr/bin/env python3
# =====================================================================
# 文件名: telecom/ddos-detection/generator/netflow_generator.py
# 所属模块: 场景二 —— NetFlow 流量记录生成器（模拟流量探针/分光采集）
# 运行方式:
#   python3 netflow_generator.py                          # 正常流量
#   python3 netflow_generator.py --scenario synflood      # 注入 SYN Flood 攻击
# 说明:
#   - NetFlow v5/v9 语义子集：源/目的 IP、端口、TCP flags、包数字节数。
#   - key=源IP：Flink CEP 按源 IP 分组匹配（同一攻击源的包进同一分区）。
#   - synflood 剧本：单个伪造源高速发 SYN（flags=S），
#     配合 lab 的 CEP 检测（60s 内 ≥5 次 → DDoS 告警）。
# =====================================================================
import argparse
import json
import random
import time

from confluent_kafka import Producer

BOOTSTRAP = "localhost:39092,localhost:39093,localhost:39094"
TOPIC = "telecom-netflow"

GOOD_IPS = [f"198.51.100.{i}" for i in range(1, 30)]     # 正常用户网段
ATTACKER_IP = "203.0.113.88"                              # 攻击源（RFC5737 测试网段）


def normal_flow(ts: int) -> dict:
    """正常流量：完整握手（SA）或数据包（PA），目的端口分布真实。"""
    return {
        "ts": ts,
        "src_ip": random.choice(GOOD_IPS),
        "dst_ip": f"10.10.0.{random.randint(1, 20)}",
        "dst_port": random.choice([443, 80, 5060, 8080]),
        "flags": random.choice(["SA", "PA", "FA"]),
        "packets": random.randint(5, 500),
        "bytes": random.randint(500, 500000),
    }


def syn_flood_flow(attacker: str, ts: int) -> dict:
    """SYN Flood：伪造源，只发 SYN 不回 ACK，目的端口集中 80。"""
    return {
        "ts": ts,
        "src_ip": attacker,
        "dst_ip": f"10.10.0.{random.randint(1, 5)}",   # 打集中目标
        "dst_port": 80,
        "flags": "S",                                   # 纯 SYN（握手第一步，永不完成）
        "packets": 1,
        "bytes": random.randint(40, 80),               # SYN 包很小：高包低字节是识别特征
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="NetFlow 生成器")
    parser.add_argument("--interval", type=float, default=0.1)
    parser.add_argument("--scenario", choices=["normal", "synflood"], default="normal")
    parser.add_argument("--attacker", default=ATTACKER_IP)
    args = parser.parse_args()

    p = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 10,
                  "compression.type": "lz4"})
    print(f"NetFlow 生成器启动（scenario={args.scenario}）")
    seq = 0
    try:
        while True:
            ts = int(time.time() * 1000)
            # synflood 模式：正常流量 + 高速攻击流混合（检测器要在噪声中识别攻击）
            if args.scenario == "synflood" and seq % 3 == 0:
                rec = syn_flood_flow(args.attacker, ts)
                key = args.attacker
            else:
                rec = normal_flow(ts)
                key = rec["src_ip"]
            p.produce(TOPIC, key=key.encode(),
                      value=json.dumps(rec).encode())
            p.poll(0)
            seq += 1
            if seq % 100 == 0:
                print(f"已生成 {seq} 条 flow")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        p.flush(10)
        print(f"退出，共 {seq} 条")


if __name__ == "__main__":
    main()
