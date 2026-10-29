#!/usr/bin/env python3
# =====================================================================
# 文件名: bigdata/kafka/scripts/seq_write_benchmark.py
# 所属模块: 模块 15 —— "顺序写为什么是 Kafka 的设计基石"对比实验
# 运行方式: python3 seq_write_benchmark.py
# 说明:
#   - 对比两种真实存储引擎的写入模式（各写 128MB，每 1MB 强制 fsync）：
#       A. 顺序追加写 —— Kafka 的 .log 追加模式
#       B. 随机读改写 —— 传统 B 树/LSM 存储引擎更新一页的模式
#   - 诚实结论（重要，实验后请阅读脚本末尾说明）：
#       * 在机械盘 / SATA SSD 时代，随机 4K 写比顺序写慢 1~2 个数量级，
#         这是 Kafka 2011 年论文选择"纯顺序日志"的历史背景；
#       * 在现代 NVMe（如 Apple M 系列统一存储）上，随机小写性能已大幅
#         提升，两者的差距可能很小甚至持平——这并不推翻 Kafka 的设计，
#         而是说明今天 Kafka 的吞吐优势来自"顺序写 + 批处理 + 零拷贝 +
#         页缓存"的乘法效应，且顺序写仍保证了大扇出复制与恢复的稳定性。
#   - 受 APFS 写时复制影响，macOS 本机数值波动较大；在 Linux ext4 上
#     （如容器内挂载卷）复测更能反映服务器磁盘的真实行为。
# =====================================================================
import os
import random
import shutil
import time

BLOCK = 4096                      # 4KB：一条消息/一个数据页的典型大小
TOTAL_BYTES = 128 * 1024 * 1024   # 每种模式写 128MB
SYNC_EVERY = 256                  # 每 256 块（1MB）强制刷盘，模拟事务提交
ROUNDS = TOTAL_BYTES // BLOCK
TMP_DIR = "/tmp/kafka-lab-seqwrite"
RECORD = b"x" * BLOCK
FILE_SIZE = 512 * 1024 * 1024     # 随机模式的工作区（大于写入量，制造"散布"）


def bench_sequential(path: str) -> float:
    """A. 顺序追加写：偏移只增不减，定期 fsync。"""
    with open(path, "ab") as f:
        start = time.perf_counter()
        for i in range(ROUNDS):
            f.write(RECORD)
            if i % SYNC_EVERY == 0:
                f.flush()
                os.fsync(f.fileno())
        f.flush()
        os.fsync(f.fileno())
        return time.perf_counter() - start


def bench_random_rmw(path: str) -> float:
    """B. 随机读改写：随机选页 → 读出 → 改写 → 定期 fsync。
    这是 B 树存储引擎（MySQL InnoDB/传统 MQ）更新索引页的真实 I/O 模式。"""
    with open(path, "w+b") as f:
        f.truncate(FILE_SIZE)          # 预分配工作区
        pages = FILE_SIZE // BLOCK
        start = time.perf_counter()
        for i in range(ROUNDS):
            offset = random.randrange(pages) * BLOCK
            f.seek(offset)
            f.read(BLOCK)              # 先读旧页（存储引擎要读出整页再改）
            f.seek(offset)
            f.write(RECORD)            # 写回整页
            if i % SYNC_EVERY == 0:
                f.flush()
                os.fsync(f.fileno())
        f.flush()
        os.fsync(f.fileno())
        return time.perf_counter() - start


def main() -> None:
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    os.makedirs(TMP_DIR)
    seq_path = os.path.join(TMP_DIR, "sequential.log")
    rnd_path = os.path.join(TMP_DIR, "random-rmw.db")

    t_seq = bench_sequential(seq_path)
    t_rnd = bench_random_rmw(rnd_path)

    def report(name: str, secs: float) -> None:
        mbps = TOTAL_BYTES / 1024 / 1024 / secs
        iops = ROUNDS / secs
        print(f"{name}: {secs:6.2f}s  吞吐 {mbps:8.1f} MB/s  IOPS {iops:9.0f}")

    report("A 顺序追加写       ", t_seq)
    report("B 随机读改写(B树式)", t_rnd)
    print(f"\nB 耗时是 A 的 {t_rnd / t_seq:5.2f} 倍")
    print(
        "\n解读指引（对照实验目的阅读）：\n"
        "  - 若 B 明显慢于 A（HDD/SATA SSD 上常见 10~100 倍）：直接印证 Kafka\n"
        "    选择纯顺序日志的历史动机；\n"
        "  - 若两者接近（现代 NVMe 上常见）：说明 Kafka 今天的吞吐优势来自\n"
        "    顺序写+批处理+零拷贝+页缓存的组合拳，且顺序写让副本复制与\n"
        "    故障恢复的 I/O 模式完全可预测。\n"
        "  - 建议在 Linux ext4 环境复测以消除 APFS 写时复制的干扰。"
    )
    shutil.rmtree(TMP_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()
