# 场景二：DDoS 攻击检测与自动防御

> **融合点**：NetFlow 生成器 → Flink CEP 攻击模式检测（模块 19）→ Agent 经 MCP 下发防火墙规则（模块 29）→ Spark 离线攻击画像（模块 21-22）→ 权限分级（模块 29 权限矩阵）
> **预估学习时间**：7 小时 | **优先级**：P0

---

## 1. 业务与架构

DDoS 防御的完整闭环分四步：**检测（现在发生了什么）→ 决策（该挡谁）→ 处置（挡住它）→ 溯源（以后怎么更快）**。本场景把四步分别落在四类组件上：

```
┌──────────┐ NetFlow  ┌──────────┐  DDoS告警  ┌───────────────────────────┐
│ 流量探针  │─────────►│ Kafka    │──────────►│ Flink CEP（m19 已验证）     │
│ (生成器)  │ telecom-netflow     │           │ 60s 内同源 ≥5 次 SYN        │
└──────────┘          └──────────┘           └──────────┬────────────────┘
     ▲                                                  ▼ telecom-alerts
     │                                       ┌─────────────────────────┐
     │                                       │ DDoS Responder（Agent）  │
     │                                       │ ① 查封禁列表(read)       │
     │                                       │ ② 下发封禁(write)        │
     │                                       │ ③ LLM 事件报告           │
     │                                       └──────────┬──────────────┘
     │                                                  │ MCP
     │                                                  ▼
     │                                       ┌─────────────────────────┐
     └────── Spark 离线攻击画像 ◄── Kafka ── │ firewall-mcp-server      │
              (谁打的/什么模式/特征)          │ add_firewall_rule(write) │
                                            │ emergency_blackhole(危险) │
                                            └─────────────────────────┘
```

**权限分级是自动防御的合规骨架**（全部实测验证）：

| 工具 | 级别 | admin | engineer | 设计理由 |
|---|---|---|---|---|
| list_blocked_ips | read | ✓ 自动 | ✓ 自动 | 只读无风险 |
| add_firewall_rule | write | ✓ 自动 | 挂起审批 | 影响业务流量，需授权 |
| emergency_blackhole | dangerous | ✗ 拒绝 | ✗ 拒绝 | 整机牵引影响全部业务，Agent 不可自动执行 |

实测输出（`--role admin`）：`防火墙已下发 drop 规则: 203.0.113.88（30 分钟后自动解除）`；`--role engineer`：`[需人工审批] ... 挂起`。**"Agent 提议、权限裁决、执行留痕"** 三段式 = 安全生产线的标准答案。

**防御规则的三条工程细节**（firewall_mcp_server.py 实现）：
1. **IP 格式校验**：LLM 给的参数不可信（模块 28 Q5 的落地）；
2. **幂等**：重复封禁不重复添加（告警风暴会触发多次响应）；
3. **自动过期**：30 分钟自动解除，防"封了忘解"的二次事故。

## 2. 实时层 vs 离线层的分工（Lambda 架构缩影）

| | Flink CEP（实时层） | Spark 离线（离线层） |
|---|---|---|
| 时延 | 秒级（within 窗口关闭即告警） | 小时/天级批处理 |
| 职责 | 挡住正在发生的攻击 | 攻击画像：Top 源 IP、波次分布、特征对比 |
| 输出 | telecom-alerts 告警事件 | 分析表（供规则调参与情报沉淀） |
| 数据 | 全量流（窗口内） | Kafka 全量历史（earliest→latest） |

**离线分析的价值**：实时检测只知道"谁在攻击"，离线层回答"攻击与正常流量的特征差异"（本项目对比了 avg_bytes/avg_packets——SYN Flood 的"高包低字节"特征就是检测规则调参的证据）。

## 3. 文件清单

| 文件 | 说明 |
|---|---|
| [generator/netflow_generator.py](generator/netflow_generator.py) | NetFlow 生成器（normal/synflood 剧本） |
| bigdata/flink `CepIntrusionJob` | SYN Flood 检测（已验证：`syn_count:6` 告警） |
| [responder/firewall_mcp_server.py](responder/firewall_mcp_server.py) | ★ 防火墙 MCP Server（三级权限/幂等/过期） |
| [responder/ddos_responder.py](responder/ddos_responder.py) | ★ 自动防御工作流（多 MCP Server 编排） |
| [jobs/offline_attack_analysis.py](jobs/offline_attack_analysis.py) | Spark 离线攻击画像 |
| [labs/lab-ddos.md](labs/lab-ddos.md) | 实验指南（全部含实测输出） |

## 4. 快速开始

```bash
# 基础设施
cd docker && docker compose -f docker-compose.kafka.yml -f docker-compose.flink.yml \
  -f docker-compose.spark.yml up -d && cd ..

# 1. 提交 CEP 检测
docker exec flink-jobmanager /opt/flink/bin/flink run -d \
  -c com.teleops.flink.m19.CepIntrusionJob /opt/flink/usrlib/teleops-flink-jobs.jar

# 2. 注入攻击流量（正常流量+SYN 洪水混合）
python3 telecom/ddos-detection/generator/netflow_generator.py --scenario synflood

# 3. 观察 CEP 告警（60s 窗口关闭后输出，记得注入"推进事件"或持续流量）
docker logs ai-native-ops-flink-taskmanager-1 2>&1 | grep DDoS告警

# 4. Agent 自动防御（admin 角色自动放行）
python3 telecom/ddos-detection/responder/ddos_responder.py \
  --attack-ip 203.0.113.88 --role admin

# 5. 验证封禁生效
cat /tmp/firewall_rules.json | python3 -m json.tool
```

## 5. 面试串讲（3 分钟版）

1. **检测**：Flink CEP 按源 IP 分组，60s 内 ≥5 次纯 SYN → 告警；开放量词 + watermark 的输出语义（模块 19 实证）；
2. **决策与处置**：Agent 经 MCP 下发规则——权限矩阵分级（read/write/dangerous）、幂等、自动过期、IP 校验四道防线；
3. **溯源**：Spark 离线对比攻击流与正常流特征（高包低字节、纯 SYN 占比），反哺检测阈值；
4. **一个能体现深度的细节**：为什么 blackhole 不开放给 Agent——告警风暴时"先拉黑再说"的人祸风险比攻击本身更危险，硬边界（deny 不可审批）是最后的保险。
