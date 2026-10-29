# 模块 25：端到端数据管道（采集 → 实时 → 离线 → 存储 → 服务）

> **前置依赖**：模块 15-24 + 场景一二 | **优先级**：P1
> **定位**：把前 24 个模块的组件串成一条完整的通信网络监控管道——本项目的"毕业设计"。

## 管道全景（所有环节均已在场景一/二实测验证）

```
┌─ 采集层 ─────────────┐
│ kpi_generator.py     │──K─► telecom-gnb-kpi ──┐
│ netflow_generator.py │──K─► telecom-netflow ──┤
│ syslog_producer.py   │──K─► telecom-gnb-alarms┤    K=Kafka(RF=3,min.isr=2)
└──────────────────────┘                        ▼
┌─ 实时层（Flink）────────────────────────────────────────────┐
│ KafkaKpiAlertJob：30s 窗口 → 分切片 SLA 判定 → 告警          │
│ CepIntrusionJob：60s 内 ≥5 SYN → DDoS 告警                   │
│ EsAlertPipelineJob：链路抖动 CEP → Kafka + ES                │
└──────────────────────┬─────────────────────────┬────────────┘
                       ▼                         ▼
┌─ 存储层 ──────────────────────┐   ┌─ 服务层（Agent）─────────────────┐
│ ES telecom-alerts（检索面）    │◄──│ slice-sla Skill：ES 检索+KPI+SLA │
│ Kafka telecom-alerts（实时面） │   │ ddos_responder：MCP 下发防火墙   │
└───────────────────────────────┘   └──────────────────────────────────┘
                       ▲
┌─ 离线层（Spark local/集群）────┐
│ offline_attack_analysis.py     │  ← 攻击画像反哺检测规则
│ m22_alarm_analysis.py          │  ← 告警关联分析/MTTR 月报
└────────────────────────────────┘
```

## 一键运行顺序（每个环节的命令在各模块 lab 已验证）

```bash
# 1. 基础设施
cd docker && docker compose -f docker-compose.kafka.yml -f docker-compose.flink.yml \
  -f docker-compose.elasticsearch.yml -f docker-compose.spark.yml up -d && cd ..
./scripts/build-flink.sh

# 2. 提交实时层（3 个作业，Slot 需求 6 > 4 时提高 TM 副本或分批提交）
for cls in KafkaKpiAlertJob CepIntrusionJob; do
  docker exec flink-jobmanager /opt/flink/bin/flink run -d \
    -c com.teleops.flink.m19.$cls /opt/flink/usrlib/teleops-flink-jobs.jar
done

# 3. 采集层（三个生成器）
python3 telecom/5g-slice-monitor/generator/kpi_generator.py --scenario degrade &
python3 telecom/ddos-detection/generator/netflow_generator.py --scenario synflood &
python3 bigdata/kafka/src/syslog_producer.py --interval 0.2 &

# 4. 服务层（Agent）
python3 agent/skills/slice-sla/skill.py
python3 telecom/ddos-detection/responder/ddos_responder.py --attack-ip 203.0.113.88 --role admin

# 5. 离线层
spark-submit --master "local[4]" --jars bigdata/spark/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar,... \
  telecom/ddos-detection/jobs/offline_attack_analysis.py
```

## 管道级面试题（端到端视角，3 道速答）

1. **端到端不丢不重怎么保证？** 生产端幂等+acks=all（模块15）→ Kafka 多副本 → Flink Checkpoint+位点（模块18）→ Sink 幂等/事务（模块20）→ ES 幂等 _id。
2. **一条告警的端到端延迟是多少？** 采集 0~200ms + Kafka <10ms + Flink 窗口 30s（核心，事件时间语义决定）+ watermark 5s ≈ **35 秒量级**——想压缩只能缩窗口或改 CEP 逐事件（精度换延迟）。
3. **实时层挂了怎么办？** Flink HA（JM 主备+Checkpoint 恢复）；Kafka Lag 兜底（恢复后追平）；下游 Agent 对告警幂等消费。
