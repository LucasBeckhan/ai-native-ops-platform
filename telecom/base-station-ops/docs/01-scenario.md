# 场景三：基站智能运维（工业互联网融合）

> **融合点**：Modbus 工业协议解析（已实测）→ Kafka → Spark MLlib 故障预测（已实测 AUC 0.828）→ 多 Agent 协作（模块 32 已实测）
> **预估学习时间**：6 小时 | **优先级**：P1

## 架构与实测记录

```
┌─ 边缘层 ──────────────────────────────────────────────┐
│ 基站动环传感器（温度/湿度/功率/电压）                    │
│ modbus_simulator.py（从站）◄──modbus_client.py（采集器）│
│   实测：读输入寄存器 [336,48,2505,5310]                 │
│         =温度33.6℃/湿度48%/功率2505W/电压53.10V        │
│         写阈值寄存器 0=500 成功（0x06 回显确认）         │
└──────────────┬───────────────────────────────────────┘
               ▼ Kafka telecom-gnb-alarms（告警/特征流水）
┌─ 离线层（Spark MLlib）───────────────────────────────┐
│ train_failure_model.py：30 天窗口特征 → 故障分类      │
│   实测：AUC 0.828 / 准确率 0.817                      │
│   特征重要度：avg_temp 0.58 / min_voltage 0.15        │
│   模型导出 /tmp/teleops-failure-model（版本化存储）    │
└──────────────┬───────────────────────────────────────┘
               ▼
┌─ 智能层（多 Agent，复用模块 32 的 ops_agents.py）──────┐
│ MonitorAgent 巡检 → DiagnoseAgent 根因 → TicketAgent   │
│   实测：EVT-xxx → 光功率-28.3dBm(置信0.9) → TKT-xxx    │
└───────────────────────────────────────────────────────┘
```

## 关键技术点

1. **工业协议**：Modbus TCP 帧结构（MBAP+PDU）、功能码 03/04/06、异常码语义——手写一遍才能面试答题（代码 260 行零依赖）；
2. **特征工程**：预测性维护的信号设计（温度均值/波动、最低电压、30 天重启与告警次数、机龄）——**特征质量 > 模型复杂度**（随机森林足够）；
3. **在线推理**：Flink 加载 Spark 导出的模型实时打分（PipelineModel.load），或定时批量打分（小规模够用）；
4. **误报治理**：预测阈值按"工单处置成本 vs 故障损失"调——预测概率 >0.7 派单，0.4~0.7 进观察名单。

## 文件清单

| 文件 | 说明 | 验证状态 |
|---|---|---|
| edge-collector/modbus_client.py | Modbus TCP 客户端（03/04/06 功能码） | ✓ 实测 |
| edge-collector/modbus_simulator.py | 从站模拟器（传感器寄存器地图） | ✓ 实测 |
| ml/train_failure_model.py | Spark MLlib 故障预测训练 | ✓ 实测 AUC 0.828 |
| ../agent/advanced/multi-agent/ops_agents.py | 监控→诊断→工单三 Agent | ✓ 实测（复用） |
| labs/lab-station.md | 实验指南 | — |

## 快速开始

```bash
# 1. Modbus 协议实验（两个终端）
python3 telecom/base-station-ops/edge-collector/modbus_simulator.py
python3 telecom/base-station-ops/edge-collector/modbus_client.py --read-input 100 4

# 2. 故障预测训练
/tmp/sparkenv/bin/spark-submit --master local[4] \
  telecom/base-station-ops/ml/train_failure_model.py

# 3. 多 Agent 处置链
python3 agent/advanced/multi-agent/ops_agents.py
```
