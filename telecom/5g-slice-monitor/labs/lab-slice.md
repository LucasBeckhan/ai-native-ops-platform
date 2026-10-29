# Lab：5G 切片智能监控端到端

> **前置**：Kafka/Flink/ES 健康；jar 已构建（scripts/build-flink.sh）
> **预计耗时**：90 分钟
> **实验目标**：跑通"生成→检测→存储→巡检→展示"五段链路，亲手复现分级阈值修复。

## 实验一：劣化注入与告警（30 分钟）

```bash
docker exec flink-jobmanager /opt/flink/bin/flink run -d \
  -c com.teleops.flink.m19.KafkaKpiAlertJob /opt/flink/usrlib/teleops-flink-jobs.jar

# 新终端开告警监听
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic telecom-alerts

# 注入劣化（uRLLC 时延 14~22ms，SLA 上限 10ms）
python3 telecom/5g-slice-monitor/generator/kpi_generator.py --scenario degrade
```

**预期告警（实测）**：

```
KPI告警> {"type":"kpi_alert","device_id":"eNB-1107","slice_id":"slice-uRLLC-001",
          "avg_throughput":95.4,"avg_latency_ms":17.3,"reason":"时延 17.3>10ms",
          "severity":"critical","message":"KPI 劣化"}
```

**观察任务**：
- [ ] eMBB 切片（吞吐 ~800）**没有**告警——分级阈值生效；
- [ ] mMTC 切片（吞吐 5~8）**没有**告警——对照实验：把作业里 SLA 表改成统一 700 再跑，会看到 mMTC 全部误报（理解为什么必须分级）；
- [ ] ES 中出现 kpi_alert：`curl "localhost:9200/telecom-alerts/_search?q=kpi_alert&size=1"`。

## 实验二：Agent SLA 巡检（20 分钟）

```bash
python3 agent/skills/slice-sla/skill.py
```

**预期输出（实测）**：

```
**[Skill: slice-sla v1.0.0]** 巡检范围: slice-uRLLC-001 → 全部切片 SLA 达标

- ES 告警: 57 条
- 违规项: 0

### 分析报告（Mock 结构化输出）...
```

**解读**：degrade 停止后当前 KPI 恢复正常（mock query_kpis 返回正常值），但 ES 里有 57 条历史告警——巡检报告正确区分了"当前状态"与"历史违规"。劣化持续运行时再跑一次，违规项会 >0。

## 实验三：Grafana 大盘（20 分钟）

1. Grafana（localhost:3000）→ Connections → Data sources → Add **Elasticsearch**：
   URL `http://elasticsearch:9200`，Index `telecom-alerts,telecom-slice-kpi*`，版本 8.x；
2. Dashboards → Import → 上传 `dashboards/slice-dashboard.json`；
3. 观察：SLA 违规 stat 面板随 degrade 注入增长、uRLLC 时延曲线越过 10ms 红线。

## 清理

```bash
docker exec flink-jobmanager /opt/flink/bin/flink cancel <JobID>
```
