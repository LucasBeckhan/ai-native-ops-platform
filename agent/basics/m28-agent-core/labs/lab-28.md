# Lab 28：ReAct Agent 端到端实验

> **前置**：Mock LLM :18000；Python 依赖 httpx 已装
> **预计耗时**：75 分钟
> **实验目标**：① 观察完整 ReAct 循环的每一步；② 验证三个安全阀；③ 实现一个新工具并让 Agent 使用它。

## 实验一：多步推理全流程（20 分钟）

```bash
cd agent/basics/m28-agent-core
python3 demo_react.py
```

**预期输出（演示一，实测）**：

```
========== 演示一：多步推理（告警 → 诊断 → 结论） ==========
步骤数: 3 | 成本: $0.000000
  [1] action: query_alarms({'device_id': 'eNB-1024', 'severity': 'major', 'limit': 10})
  [1] observation: [{"alarm_id": "A-1002", "device_id": "eNB-1024", "severity": "major", "text": "光模块接收异常", ...
  [2] action: run_diagnosis({'device_id': 'eNB-1024'})
  [2] observation: {"device_id": "eNB-1024", "checked": ["cpu", "memory", "optical", "links", "config_diff"], ...
  [3] final: 已完成分析，结论如下（问题：基站 eNB-1024 出了问题，帮我查一下原因并给出处理建议）...
```

**观察任务**：
- [ ] 第 1 步选了 query_alarms——"查原因"触发了告警检索；
- [ ] 第 2 步 Agent 看到"光模块接收异常"后追加 run_diagnosis——这就是 ReAct 的"观察驱动下一步"；
- [ ] 第 3 步收敛为最终答案，循环终止。

## 实验二：三个安全阀逐一验证（25 分钟）

```python
# safety_valves.py
from react_agent import ReActAgent
from tools import ToolRegistry, Tool

# 阀 1：max_steps 兜底 —— 提供一个永远"鼓励继续查询"的场景
agent = ReActAgent(max_steps=2)
r = agent.run("全面分析 eNB-1024 的健康状况、告警、KPI 与历史故障")
print(f"[阀1] steps={r.steps_used}（达到上限后强制作答）答案前 60 字: {r.answer[:60]}")

# 阀 2：工具异常 Observation 化 —— 注册一个必抛异常的工具
reg = ToolRegistry()
def boom(**kw): raise RuntimeError("模拟下游网管超时")
reg.register(Tool(name="boom_query", description="查询必失败（演示）",
                  parameters={"type": "object", "properties": {}}, func=boom))
from tools import build_default_registry
reg2 = build_default_registry()
reg2.register(Tool(name="boom_query", description="查询必失败（演示）",
                   parameters={"type": "object", "properties": {}}, func=boom))
a2 = ReActAgent(registry=reg2, max_steps=3)
r2 = a2.run("查询 eNB-1024 的告警")   # Mock 会命中 query_alarms，异常路径见 trace
print(f"[阀2] 程序未崩溃，正常返回 {len(r2.trace)} 条轨迹")

# 阀 3：参数校验回喂 —— 直接调内部执行器模拟 LLM 传错参
obs = a2._execute_tool("query_alarms", {})   # query_alarms 无 required，改用带 required 的
obs2 = a2._execute_tool("run_diagnosis", {}) # 缺 device_id
print(f"[阀3] 校验失败的 Observation: {obs2[:60]}")
```

**预期**：三个阀全部生效（程序不崩、轨迹完整、错误信息可读）。

## 实验三：自己动手加一个工具（30 分钟）

目标：新增 `get_change_tickets(device_id)` 工具（查询设备近期变更单），并让 Mock LLM 能路由到它。

1. 在 `tools.py` 中实现函数与注册（参考 query_alarms 的写法，返回 2 条硬编码变更单）；
2. 在 `mock-llm/mock_llm_server.py` 的 `_pick_tool` 规则表加一行：`("变更", ["get_change_tickets"])`；
3. 重启 Mock LLM，运行：
   ```bash
   python3 -c "
   from react_agent import ReActAgent
   print(ReActAgent().run('eNB-1024 最近有变更吗？').answer[:100])"
   ```
4. **验收**：轨迹中出现 `get_change_tickets` 的 action。

这个练习完整走通"工具三要素（Schema/实现/描述）+ 路由关键词"的扩展路径——生产上加工具就是这个流程。
