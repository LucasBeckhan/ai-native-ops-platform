#!/usr/bin/env python3
# =====================================================================
# 文件名: bigdata/kafka/scripts/explore_storage.py
# 所属模块: 模块 15 —— Kafka 存储机制实验辅助（.log/.index/.timeindex 探查）
# 运行方式:
#   python3 explore_storage.py --ls                 # 列出集群所有分区的磁盘目录
#   python3 explore_storage.py --dump telecom-gnb-kpi 0   # 十六进制查看某个 .log 段文件头部
# 说明:
#   - 容器内数据目录 /bitnami/kafka（本镜像为 /var/lib/kafka/data 之外还有
#     官方镜像布局差异），脚本统一通过 docker exec 在 kafka-1 容器内执行，
#     不要求学习者进入容器操作。
#   - 目标：亲眼确认"顺序追加的 .log + 稀疏索引 .index + 时间索引 .timeindex"
#     三件套真实存在，并理解 Segment 滚动（1GB/7天，以先到者为准）。
# =====================================================================
import argparse
import subprocess
import sys

CONTAINER = "kafka-1"
# apache/kafka 官方镜像的日志根目录（log.dirs 默认 /tmp/kafka-logs）
LOG_ROOT = "/tmp/kafka-logs"  # apache/kafka 4.x 官方镜像默认 log.dirs


def sh(cmd: str) -> str:
    """在 kafka-1 容器内执行 shell 命令并返回 stdout。"""
    result = subprocess.run(
        ["docker", "exec", CONTAINER, "bash", "-c", cmd],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"命令失败: {cmd}\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    return result.stdout


def list_partition_dirs() -> None:
    """列出所有分区目录及其段文件，验证磁盘布局。"""
    print(f"== 数据目录: {LOG_ROOT}")
    dirs = sh(f"ls {LOG_ROOT} | grep -E '^[a-zA-Z].*-[0-9]+$' | sort")
    for d in dirs.split():
        files = sh(
            f"ls -lh {LOG_ROOT}/{d} | awk '{{print $5, $9}}' | head -8"
        )
        print(f"\n分区目录: {d}")
        print(files.rstrip())


def dump_log_head(topic: str, partition: int) -> None:
    """十六进制查看首个 Segment 的 .log 头部与 .index 内容。

    Kafka 的 .log 是二进制格式（RecordBatch 头含 baseOffset/lastOffset/
    magic/compression 等字段）。这里不做完整协议解析——肉眼看到
    "offset 连续、batch 头部结构" 即达到实验目的。
    """
    base = f"{LOG_ROOT}/{topic}-{partition}/00000000000000000000"
    print(f"== {base}.log 前 256 字节（RecordBatch 二进制头）:")
    print(sh(f"xxd {base}.log | head -16"))
    print("== .index 文件（稀疏索引，offset→物理位置，每条 8 字节）:")
    print(sh(f"xxd {base}.index | head -8"))
    print("== .timeindex 文件（时间戳→offset，每条 12 字节）:")
    print(sh(f"xxd {base}.timeindex | head -6"))
    print("== 本分区全部 Segment 文件（观察滚动）:")
    print(sh(f"ls -lh {LOG_ROOT}/{topic}-{partition}/ | grep -E '\\.(log|index|timeindex)$'"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka 存储探查工具")
    parser.add_argument("--ls", action="store_true", help="列出所有分区目录")
    parser.add_argument("--dump", nargs=2, metavar=("TOPIC", "PARTITION"))
    args = parser.parse_args()
    if args.ls:
        list_partition_dirs()
    elif args.dump:
        dump_log_head(args.dump[0], int(args.dump[1]))
    else:
        parser.print_help()
