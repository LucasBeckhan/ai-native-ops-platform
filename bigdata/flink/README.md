# Flink 系列模块（18/19/20）：架构概念 · 流处理实战 · 调优运维

> **前置依赖**：模块 15-17（Kafka）
> **预估学习时间**：18 小时 | **优先级**：P0

## 学习目标

- 18：时间语义/Watermark（含空闲分区冻结的实测教训）、窗口、Keyed State、Checkpoint 恢复（含故障注入实验）
- 19：Kafka Connector 生产配置、实时 KPI 告警作业、CEP 攻击检测（含开放量词 watermark 语义实证）
- 20：反压定位、数据倾斜两次聚合、Checkpoint 调优、完整监控管道（Kafka→CEP→Kafka+ES，实测 ES 索引 113 条告警）

## 文件清单

```
bigdata/flink/
├── pom.xml                        # Java 8 目标 + provided 依赖策略（瘦作业 jar）
├── docs/
│   ├── 01-concepts.md             # 模块18 知识文档
│   ├── 02-streaming-cep.md        # 模块19 知识文档
│   ├── 03-tuning.md               # 模块20 知识文档
│   └── interview-m18-m20.md       # Flink 面试题 9 道
├── labs/{lab-18,lab-19,lab-20}.md # 实验指南（全部含真实预期输出）
└── src/main/java/com/teleops/flink/
    ├── m18/EventTimeWindowJob.java      # 乱序+Watermark+事件时间窗口（已验证）
    ├── m18/StateCheckpointJob.java      # 故障注入→Checkpoint 恢复（已验证）
    ├── m19/KafkaKpiAlertJob.java        # Kafka→窗口→阈值→Kafka（已验证）
    ├── m19/CepIntrusionJob.java         # CEP SYN Flood 检测（已验证，输出 DDoS 告警）
    └── m20/EsAlertPipelineJob.java      # 完整管道→ES（已验证，ES 索引 113 条）
```

## 快速启动

```bash
cd docker && docker compose -f docker-compose.kafka.yml -f docker-compose.flink.yml up -d
cd .. && ./scripts/build-flink.sh          # 容器化离线构建（依赖走宿主 ~/.m2 缓存）
bigdata/flink/labs/lab-18.md               # 按实验指南逐个提交作业
```

## 实测记录（生成时验证结论，学习者应能复现）

| 实验 | 结果 |
|---|---|
| 事件时间窗口（乱序 0~300ms） | 42 个窗口正确聚合，边界整秒对齐 |
| 故障注入 + Checkpoint | 崩溃→3s 重启→状态续跑，输出含两次"累计 20 条" |
| CEP SYN Flood | 输出 `{"attack_type":"SYN_Flood","syn_count":6}` |
| 完整管道→ES | telecom-alerts 索引 113 条 link_flap 告警 |
