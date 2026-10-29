#!/usr/bin/env python3
# =====================================================================
# 文件名: telecom/5g-slice-monitor/generator/kpi_generator.py
# 所属模块: 场景一 —— 5G 基站 KPI 数据生成器（模拟网管北向接口采集）
# 运行方式:
#   python3 kpi_generator.py                 # 持续生成
#   python3 kpi_generator.py --scenario degrade   # 注入切片劣化（触发告警链路）
# 说明:
#   - 模拟 10 个 gNB × 3 类切片（eMBB/uRLLC/mMTC）的周期性 KPI 上报，
#     字段与网管 3GPP PM 语义对齐：吞吐/时延/丢包/连接数/RRC 建立成功率。
#   - key=切片ID：保证同一切片的 KPI 时序在分区内有序（Flink 窗口正确性的前提）。
#   - --scenario degrade 模式把 uRLLC 切片的时延拉高到 SLA 之上，
#     用于端到端验证"Flink 检测→ES→Agent SLA 分析"全链路。
# =====================================================================
import argparse
import json
import random
import time

from confluent_kafka import Producer

BOOTSTRAP = "localhost:39092,localhost:39093,localhost:39094"
TOPIC = "telecom-gnb-kpi"

DEVICES = [f"eNB-10{0:02d}".replace("000", f"{i:03d}") for i in range(100, 110)]
SLICES = {
    "slice-eMBB-001":  {"throughput": (780, 850), "latency": (15, 20), "loss": (0.0005, 0.002)},
    "slice-uRLLC-001": {"throughput": (90, 100), "latency": (4, 6),   "loss": (0.00005, 0.0002)},
    "slice-mMTC-001":  {"throughput": (5, 8),    "latency": (40, 60), "loss": (0.001, 0.005)},
}


def build_record(device: str, slice_id: str, degrade: bool, ts: int) -> dict:
    rng = SLICES[slice_id]
    throughput = random.uniform(*rng["throughput"])
    latency = random.uniform(*rng["latency"])
    loss = random.uniform(*rng["loss"])
    # 劣化剧本：uRLLC 时延超 SLA（10ms）→ Flink KPI 告警 + Agent SLA 分析的目标场景
    if degrade and slice_id == "slice-uRLLC-001":
        latency = random.uniform(14, 22)
        loss = random.uniform(0.0004, 0.001)
    return {
        "ts": ts,
        "deviceId": device,
        "sliceId": slice_id,
        "throughput": round(throughput, 1),
        "latency_ms": round(latency, 1),
        "packet_loss": round(loss, 6),
        "rrc_setup_rate": round(random.uniform(98.5, 99.99), 2),
        "active_users": random.randint(100, 20000),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="5G 基站 KPI 生成器")
    parser.add_argument("--interval", type=float, default=0.2, help="每条间隔秒")
    parser.add_argument("--scenario", choices=["normal", "degrade"], default="normal")
    args = parser.parse_args()

    p = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,          # 采集器底线配置（模块 15/16）
        "linger.ms": 20, "compression.type": "lz4",
    })
    print(f"KPI 生成器启动（scenario={args.scenario}），Ctrl+C 停止")
    seq = 0
    try:
        while True:
            device = random.choice(DEVICES)
            slice_id = random.choice(list(SLICES))
            rec = build_record(device, slice_id, args.scenario == "degrade",
                               int(time.time() * 1000))
            # key=切片ID：同切片时序保序（模块 15 文档 3.1 铁律 2）
            p.produce(TOPIC, key=slice_id.encode(),
                      value=json.dumps(rec, ensure_ascii=False).encode())
            p.poll(0)
            seq += 1
            if seq % 50 == 0:
                print(f"已生成 {seq} 条 KPI")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        p.flush(10)
        print(f"生成器退出，共 {seq} 条")


if __name__ == "__main__":
    main()
