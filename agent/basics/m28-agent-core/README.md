# 模块 28：Agent 核心模式（ReAct / Tool Use / Planning / Memory）

> **前置依赖**：模块 26/27
> **预估学习时间**：6 小时
> **优先级**：P0

## 学习目标

1. 手写完整 ReAct 循环（130 行），说清 Function Calling 消息协议的每一条纪律；
2. 实现循环的三个安全阀（步数上限/异常 Observation 化/参数校验回喂）；
3. 实现三层 Memory（短期裁剪/长期向量检索/工作草稿）并说清各自边界；
4. 建立"工作流 vs Agent"的选型判断标准与 Agent 评估框架。

## 文件清单

| 文件 | 说明 |
|---|---|
| [docs/01-agent-core.md](docs/01-agent-core.md) | 知识文档：ReAct/FC 协议/Planning/Memory/框架对照 |
| [docs/interview-m28.md](docs/interview-m28.md) | 面试题 7 道 |
| [labs/lab-28.md](labs/lab-28.md) | 实验指南：多步推理/三安全阀/自己加工具 |
| [tools.py](tools.py) | 工具注册中心 + 三个运维工具（模块 29 复用） |
| [react_agent.py](react_agent.py) | ★ 从零实现的 ReAct Agent |
| [memory.py](memory.py) | 三层 Memory 系统 |
| [demo_react.py](demo_react.py) | 三场景演示（多步推理/异常自愈/长期记忆） |

## 快速启动

```bash
cd docker && docker compose -f docker-compose.llm.yml up -d   # Mock LLM :18000
cd ../agent/basics/m28-agent-core
python3 tools.py          # 工具自检
python3 memory.py         # 三层记忆自检
python3 demo_react.py     # 端到端演示
```

## 与后续模块的关系

tools.py 的三个工具将在模块 29 被原样封装为 MCP Server 能力；
react_agent 的循环逻辑将迁入 Harness 的 agent_runtime；
ReActResult/StepTrace 是模块 33 可观测性的数据契约。
