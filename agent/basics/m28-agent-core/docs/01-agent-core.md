# 模块 28：Agent 核心模式——ReAct、Tool Use、Planning 与 Memory

> **前置依赖**：模块 26/27
> **预估学习时间**：6 小时（文档 2h + 实验 3h + 面试题 1h）
> **配套代码**：`agent/basics/m28-agent-core/{tools.py, react_agent.py, memory.py, demo_react.py}`
> **配套实验**：`labs/lab-28.md`

---

## 1. ReAct：让 LLM 从"答题者"变成"执行者"

### 1.1 循环的骨架

```
            ┌──────────────────────────────────────────────┐
            │                                              │
用户问题 ──►│ Thought（我需要什么信息？）                     │
            │    ↓                                         │
            │ Action（调用工具：query_alarms(eNB-1024)）      │
            │    ↓                                         │
            │ Observation（工具返回 2 条告警...）             │──┐
            │    ↓                                         │  │ 信息不够？
            │ Thought（光功率异常，查一下诊断…）               │◄─┘
            │    ↓                                         │
            │ Final Answer（结论 + 建议）                    │
            └──────────────────────────────────────────────┘
```

ReAct 的本质：**把"思考过程"变成循环里的显式步骤，把"行动"变成可执行的函数调用**。相比一次性回答（幻觉重灾区），每一步行动都有真实数据校准，答案自然落地。

### 1.2 Function Calling 的消息协议（OpenAI 风格，面试必背）

一轮工具调用的完整消息序列：

```
[user]      "基站 eNB-1024 出了问题，帮我查原因"
[assistant] tool_calls: [{id:"call_1", function:{name:"query_alarms",
                       arguments:"{\"device_id\":\"eNB-1024\"}"}}]   ← 模型输出
[tool]      (tool_call_id="call_1") "[{...告警JSON...}]"              ← 你回填
[assistant] "根因最可能是光模块故障，建议..."                        ← 模型收敛
```

四条协议纪律：① `tool` 消息必须带 `tool_call_id` 与请求配对；② 参数是**字符串化的 JSON**（不是对象）；③ 工具结果也是字符串（复杂结构自行 JSON 化）；④ 本轮 assistant 的 tool_calls 消息必须原样保留在上下文里——丢了它下一轮就会 400。

### 1.3 循环的三个安全阀（react_agent.py 逐条实现）

| 安全阀 | 触发场景 | 本项目的处理 |
|---|---|---|
| 步数上限 max_steps | 模型重复调用同一工具（最常见 LLM 行为缺陷） | 达上限后注入"强制作答"提示，基于已有观察收敛 |
| 工具异常 Observation 化 | 下游接口超时/参数错 | 异常文本作为 Observation 返回，模型自行改路——程序不崩 |
| 参数校验 | LLM 传参缺字段/类型错 | 校验失败信息回喂模型，形成"自我修复闭环" |

### 1.4 Mock 环境下的确定性复现

真实 LLM 的行为不可断言，Mock LLM 的决策规则（关键词→工具、工具结果→总结）让每一步都可预测——`demo_react.py` 演示一固定为 3 步：`query_alarms → run_diagnosis → final`。这是"Agent 回归测试"的正确姿势：**测试用 Mock，验收用真模型**。

## 2. Planning：从单循环到任务分解

ReAct 适合"几步内可完成"的任务。复杂任务（"全网基站健康度周报"）需要前置规划：

- **任务分解（Decomposition）**：先让 LLM 把目标拆成子任务清单（DAG），再逐个交给 ReAct 执行——"Plan-and-Execute"模式；
- **动态重规划**：某子任务失败/返回意外结果时，重新规划剩余路径，而不是硬走原计划；
- **与工作流的边界**：路径固定的任务用工作流（代码写死的 DAG，可控可测），路径开放的任务才用 Agent（灵活但贵且慢）。**能写死的不要交给 Agent**——这是成本与可靠性的第一原则。

## 3. Memory：三层记忆的工程实现（memory.py）

| 层 | 生命周期 | 实现 | 用途 |
|---|---|---|---|
| 短期（Buffer） | 当前会话 | 消息列表 + Token 预算裁剪（保头保尾） | 维持对话连贯 |
| 长期（Vector） | 跨会话 | Embedding + 余弦检索（Mock/Chroma/Milvus 可换） | 沉淀历史案例/用户偏好 |
| 工作（Scratchpad） | 单次执行 | Thought/Action/Observation 草稿 | ReAct 中间状态 |

**裁剪策略为什么是"保头保尾"**：任务目标与系统约束在开头（丢了会跑偏），最近几轮在结尾（丢了会失忆）——中间的历史细节价值最低。**长期记忆的写入时机**：任务完成的总结、用户显式纠正、重复出现的模式——不是所有对话都值得沉淀。

## 4. 主流框架对照（选型视野）

| 能力 | LangChain/LangGraph | LlamaIndex | CrewAI | 本项目自研 |
|---|---|---|---|---|
| ReAct 循环 | AgentExecutor/LangGraph | ReActAgent | 角色化自动 | ✓ 130 行手写 |
| 工具管理 | 大量集成器 | QueryTool 为主 | 内置 | ✓ ToolRegistry |
| 记忆 | 多种 Memory 类 | 索引体系 | 短期为主 | ✓ 三层最小实现 |
| 多 Agent | LangGraph 图编排 | 弱 | 强（角色分工） | 模块 32 |
| 学习价值 | 看抽象设计 | 看数据接入 | 看角色编排 | ★ 看穿本质 |

**面试话术**：先讲手写实现证明理解本质（"我的 ReAct 循环 130 行，三个安全阀怎么处理"），再讲框架选型逻辑（"LangGraph 的图编排适合有状态长流程，CrewAI 适合角色化分工"）——自研与框架双修是高级候选人的标配。

## 自检四问

1. Function Calling 的 tool 消息为什么必须带 tool_call_id？（1.2 协议纪律）
2. max_steps 兜底时注入的"强制作答"包含什么？为什么？（1.3 + scratch.render()）
3. 保头保尾裁剪的依据？（第 3 节）
4. 什么任务不该用 Agent？（第 2 节：路径固定用工作流）
