# 模块 29（核心）：自研 Agent Harness —— Skill vs MCP vs Harness

> **前置依赖**：模块 26/27/28
> **预估学习时间**：8 小时
> **优先级**：P0 · 全项目架构认知核心

## 学习目标

1. 精确说出 Harness/Skill/MCP 三者的定位、边界与协作链路（面试一级考点）；
2. 亲手实现 Harness 四大件（上下文/路由/权限/运行时），理解"舞台-剧本-接口"的分层；
3. 手写 MCP 协议（JSON-RPC 2.0，stdio+HTTP 双传输），掌握 tools/resources/prompts 三能力；
4. 跑通"直接函数调用 vs MCP"对比实验并给出工程选型结论。

## 文件清单

```
agent/harness/
├── README.md
├── docs/
│   ├── 01-skill-mcp-harness.md    # ★ 架构主文档（三层模型/六大职责/对比实验结论）
│   └── interview-m29.md           # ★ Skill/MCP/Harness 专题 11 道必出面试题
├── labs/                          # 见 mcp/labs/lab-29.md（端到端）
└── src/
    ├── context_manager.py         # 组件一：多会话上下文（隔离/事实沉淀/压缩）
    ├── permission.py              # 组件二：角色×级别权限矩阵 + 审批流 + 审计
    ├── tool_router.py             # 组件三：本地/MCP 统一路由（唯一执行通道）
    ├── agent_runtime.py           # 组件四：会话生命周期 + ReAct 总装
    └── skill_manager.py           # Skill 发现/匹配/执行
```

## 快速启动

```bash
cd docker && docker compose -f docker-compose.llm.yml up -d    # Mock LLM :18000
cd ../agent/harness/src
python3 permission.py        # 权限矩阵自检（6 种角色×级别组合）
python3 skill_manager.py     # Skill 发现与匹配
cd ../../mcp/minimal && python3 mcp_client.py        # MCP 双传输自检
cd ../examples && python3 e2e_demo.py                # ★ 端到端 + 对比实验
```
