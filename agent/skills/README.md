# 通信行业 Skill 集（模块 29 配套）

三个自包含 Skill，展示"确定性工作流 + 少量 LLM 判断"的领域封装范式。

| Skill | 触发词 | 工具依赖 | 关键设计 |
|---|---|---|---|
| [alarm-analysis](alarm-analysis/) | 告警/故障/退服/异常/中断 | query_alarms, run_diagnosis | 数据全走 Router；LLM 只做综合判断 |
| [config-audit](config-audit/) | 配置/审计/基线/合规/整改 | run_diagnosis | 规则判定用代码不用 LLM（可审计） |
| [traffic-diagnosis](traffic-diagnosis/) | 流量/KPI/吞吐/时延/切片/丢包 | query_kpis + MCP resources | SLA 阈值经 MCP Resource 读取 |

## 结构约定（新增 Skill 照此办理）

```
skills/<name>/
├── skill.json   # 元数据：name/version/description/triggers/permissions/tools
└── skill.py     # 必须暴露 run(ctx) 入口；ctx 提供 router/llm/perm/facts
```

运行时由 `agent/harness/src/skill_manager.py` 自动发现加载，新增目录零改动。
进阶 Skill（拓扑可视化/容量规划）见模块 30-34 的扩展任务。
