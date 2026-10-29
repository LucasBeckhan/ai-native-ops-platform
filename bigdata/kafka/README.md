# Kafka 系列模块（15/16/17）：架构原理 · 生产消费实战 · 集群运维调优

> **前置依赖**：模块 15 无前置；模块 16 依赖 15；模块 17 依赖 15/16
> **预估学习时间**：15 小时（每模块约 5 小时）
> **优先级**：P0（面试最高频 + 本项目所有数据链路的地基）

## 学习目标

- **模块 15（架构原理）**：三层逻辑视图、KRaft 元数据体系、存储三件套、ISR/LEO/HW、不丢消息底线配置。
- **模块 16（生产消费实战）**：分区策略与保序、幂等/事务生产者、消费组与再平衡、位点多义性，完成设备日志采集管道。
- **模块 17（运维调优）**：三节点集群的容量规划、关键监控指标（URP/Lag/延迟）、故障演练与分区重分配。

## 文件清单

```
bigdata/kafka/
├── README.md                      # 本文件
├── docs/
│   ├── 01-architecture.md         # 模块15 知识文档
│   ├── 02-producer-consumer.md    # 模块16 知识文档
│   ├── 03-ops-tuning.md           # 模块17 知识文档
│   ├── interview-m15.md           # 模块15 面试题 8 道
│   ├── interview-m16.md           # 模块16 面试题 8 道
│   └── interview-m17.md           # 模块17 面试题 7 道
├── labs/
│   ├── lab-15-storage-and-isr.md  # 存储三件套 + ISR 收缩演练
│   ├── lab-16-pipeline.md         # 设备日志采集管道（幂等/事务/再平衡）
│   └── lab-17-ops.md              # Broker 故障演练 + 分区重分配 + Lag 排查
├── scripts/                       # 命令行实验工具
│   ├── create-topics.sh           # 全项目 Topic 统一创建（含底线配置）
│   ├── explore_storage.py         # .log/.index/.timeindex 探查
│   └── seq_write_benchmark.py     # 顺序写 vs 随机写基准
└── src/                           # Python 客户端实验代码（模块 16）
    ├── requirements.txt
    ├── syslog_producer.py         # 模拟网管采集器：Syslog/SNMP Trap → Kafka
    ├── producer_idempotent.py     # 幂等 + 重试 + 保序验证
    ├── producer_transaction.py    # 事务生产者（多分区原子写）
    ├── consumer_group_demo.py     # 消费组 + 手动位移提交 + 再平衡监听
    └── consumer_idempotent.py     # 幂等消费（业务主键去重）
```

## 快速启动

```bash
# 1. 启动三节点 KRaft 集群（详见 docker/docker-compose.kafka.yml 头部注释）
cd docker && docker compose -f docker-compose.kafka.yml up -d

# 2. 创建全部业务 Topic
cd ../bigdata/kafka/scripts && chmod +x create-topics.sh && ./create-topics.sh

# 3. 进入各模块：先读 docs/，再按 labs/ 实验，最后复盘 interview-*.md
```

## 依赖环境

Python 3.11 + `pip install -r src/requirements.txt`（confluent-kafka 客户端，
生产主流、支持事务与幂等 API，带 Apple Silicon 预编译 wheel）。
