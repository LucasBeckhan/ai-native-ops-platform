# 场景一：5G 网络切片智能监控

> **融合点**：Kafka（模块 15-17）→ Flink 分切片 SLA 检测（模块 18-20）→ Elasticsearch → slice-sla Skill（模块 29）→ Helm/HPA 部署（K8s 模块 2/7/10）
> **预估学习时间**：6 小时 | **优先级**：P0

---

## 1. 业务与架构

5G 网络切片把一张物理网切成多张逻辑网：eMBB（大带宽）、uRLLC（低时延）、mMTC（大连接），各自有不同的 SLA 承诺。切片 SLA 监控 = **按切片分级的指标检测 + 违规告警 + 智能巡检报告**。

```
┌─────────┐  KPI   ┌─────────────┐  原始流   ┌────────────────────────────┐
│ gNB ×10 │───────►│ Kafka       │─────────►│ Flink KafkaKpiAlertJob     │
│ 3类切片  │        │ telecom-gnb-kpi          │  30s 事件时间窗口           │
└─────────┘        └─────────────┘          │  分切片 SLA 阈值判定         │
                                            │  eMBB:吞吐≥700 / uRLLC:时延≤10ms │
                                            └──────┬──────────────┬──────┘
                                                   ▼              ▼
                                          telecom-alerts     telecom-alerts
                                           (Kafka,Agent)      (ES,检索)
                                                                   │
                        ┌──────────────────────────────────────────┘
                        ▼
              slice-sla Skill（经 MCP es_query_alerts 拉告警 + query_kpis 拉实时 KPI
                              → 代码判定 SLA 违规 → LLM 巡检报告）
```

**三条设计原则**（本场景反复出现，面试可整体引用）：
1. **分级阈值**：固定阈值会把 mMTC（天然吞吐 5-8Mbps）全部误报——本项目第一版实现的真实教训，修为每切片 SLA 表（吞吐下限+时延上限）；
2. **告警双写**：Kafka 给实时消费者（Agent/工单），ES 给检索面（Skill 巡检/人查），一份数据两种访问模式；
3. **判定用代码、叙述用 LLM**：SLA 违规判定在 Flink 算子（确定性），Skill 里再次用代码复核，LLM 只写报告（可审计，模块 29 原则）。

## 2. 文件清单

| 文件 | 说明 |
|---|---|
| [generator/kpi_generator.py](generator/kpi_generator.py) | KPI 生成器（normal/degrade 两种剧本） |
| bigdata/flink `KafkaKpiAlertJob` | 分切片 SLA 检测（已验证：uRLLC 时延 17.3>10ms 告警） |
| agent/skills/slice-sla/ | ★ 切片 SLA 分析 Skill（已验证：ES 57 条告警 + 实时 KPI 巡检） |
| agent/mcp `es_query_alerts` | MCP Server 新增的 ES 检索工具（工具面扩展实例） |
| [dashboards/slice-dashboard.json](dashboards/slice-dashboard.json) | Grafana 大盘（导入即用） |
| [deploy/manifests/](deploy/manifests/) | K8s 清单（Flink Application 模式 + 生成器） |
| [deploy/manifests/hpa.yaml](deploy/manifests/hpa.yaml) | HPA（生成器 CPU 弹性，模拟话务洪峰） |
| [deploy/helm/](deploy/helm/) | Helm Chart（values 参数化多环境） |
| [labs/lab-slice.md](labs/lab-slice.md) | 实验指南（全部含实测输出） |

## 3. 快速开始

```bash
# 基础设施
cd docker && docker compose -f docker-compose.kafka.yml -f docker-compose.flink.yml \
  -f docker-compose.elasticsearch.yml up -d && cd ..

# 提交检测作业（jar 构建见 scripts/build-flink.sh）
docker exec flink-jobmanager /opt/flink/bin/flink run -d \
  -c com.teleops.flink.m19.KafkaKpiAlertJob /opt/flink/usrlib/teleops-flink-jobs.jar

# 注入劣化流量（uRLLC 时延拉到 14~22ms）
python3 telecom/5g-slice-monitor/generator/kpi_generator.py --scenario degrade

# 观察告警（新终端）
docker exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic telecom-alerts
```

**预期告警（实测）**：

```json
{"type":"kpi_alert","device_id":"eNB-1107","slice_id":"slice-uRLLC-001",
 "avg_latency_ms":17.3,"reason":"时延 17.3>10ms","severity":"critical","message":"KPI 劣化"}
```

然后跑 Skill 巡检（`python3 agent/skills/slice-sla/skill.py`）与 Grafana 大盘（导入 dashboards JSON，ES 数据源需在 Grafana 里补配 elasticsearch:9200）。

## 4. K8s 部署（Helm + HPA）

```bash
# kind 集群就绪后（k8s/basics/01 的 lab）：
kubectl create namespace teleops
helm install slice1 telecom/5g-slice-monitor/deploy/helm -n teleops
kubectl apply -f telecom/5g-slice-monitor/deploy/manifests/hpa.yaml -n teleops
# 压测生成器 → 观察 HPA 扩容：kubectl get hpa -n teleops -w
```

注意：Flink 有状态作业本身不走 HPA（扩缩需 Savepoint 迁移，模块 10 讲 KEDA 按 Kafka Lag 伸缩消费者的正确姿势）；HPA 管的是无状态的采集端。

## 5. 面试串讲（把整个场景讲成 3 分钟）

1. **数据面**：基站 KPI 以切片为 key 进 Kafka（保序）；Flink 事件时间窗口 + 分切片 SLA 判定，告警双写 Kafka/ES；
2. **智能面**：slice-sla Skill 把"ES 检索 + KPI 查询 + 规则复核 + 报告"封装为可复用能力，经 MCP 统一接入；
3. **平台面**：全部组件 K8s 化，无状态采集端 HPA 弹性，有状态 Flink 用 Checkpoint+Savepoint 保障；
4. **一个能体现深度的细节**：mMTC 误报问题 → 分级阈值 → 阈值管理的配置化方向（网管北向接口下发）。
