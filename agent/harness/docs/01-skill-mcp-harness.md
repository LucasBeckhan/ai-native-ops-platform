# 模块 29：Agent 架构核心——Skill vs MCP vs Harness

> **前置依赖**：模块 26/27/28
> **预估学习时间**：8 小时（文档 2.5h + 实验 4h + 面试题 1.5h）
> **本模块地位**：Agent 方向的架构认知核心，从"会用框架"到"能设计系统"的分水岭，也是面试区分初级与高级候选人的主战场。
> **配套代码**：`agent/harness/`（自研 Harness）、`agent/skills/`（3 个通信 Skill）、`agent/mcp/`（手写 MCP Server/Client）、`agent/mcp/examples/e2e_demo.py`（端到端联调 + 对比实验）

---

## 1. 三个概念的正名：一句话定位

| 概念 | 一句话定位 | 类比 |
|---|---|---|
| **Harness** | Agent 的**运行时环境/执行容器**：生命周期、上下文、工具调度、权限、I/O、可观测 | 操作系统（OS） |
| **Skill** | **封装好的能力单元**：一组工具+提示词+工作流的领域打包 | 应用程序（App） |
| **MCP** | **标准化通信协议**：Agent 与外部工具/数据源的统一接入方式 | USB-C 接口标准 |

三者回答的是三个不同的问题：Harness 回答"Agent **在哪里**运行、被什么约束"，Skill 回答"Agent **会做什么**"，MCP 回答"Agent **怎么够到**外部系统"。它们不是竞争关系，而是三个层次的叠加。

## 2. Harness：Agent 的操作系统（六大职责）

### 2.1 为什么必须有 Harness

裸跑的 ReAct 循环（模块 28）有四个致命缺口：① 没有会话边界（多用户聊天历史互相污染）；② 没有权限闸门（LLM 决定调什么就执行什么）；③ 没有审计（出了问题无法回答"Agent 当时为什么这么做"）；④ 没有生命周期（进程重启即失忆）。Harness 就是把这四件事系统化：

```
┌─────────────────────────── Harness ────────────────────────────┐
│                                                                │
│  ① 生命周期管理: create → chat×N → pause/resume → close          │
│  ② 上下文管理:   会话隔离 · 系统提示 · 用户偏好 · 事实沉淀 · 压缩   │
│  ③ 工具调度:     统一 Schema → 路由（本地函数 / MCP）→ 结果归一化   │
│  ④ 权限沙箱:     角色×工具级别矩阵（allow/deny/require_approval）  │
│  ⑤ I/O 通道:     用户输入输出、事件回调、与外部系统通信              │
│  ⑥ 可观测性:     每步决策/工具调用/Token 消耗的审计日志             │
│                                                                │
│   Agent（ReAct 循环）运行其中 —— 每次行动都在灯光与围栏之内         │
└────────────────────────────────────────────────────────────────┘
```

### 2.2 本项目的实现映射（代码导读）

| 职责 | 实现文件 | 关键机制 |
|---|---|---|
| 生命周期 | `harness/src/agent_runtime.py` | create_session/pause/resume/close + SessionReport |
| 上下文 | `harness/src/context_manager.py` | 多会话隔离 + 事实沉淀（facts）+ Token 预算压缩 |
| 工具调度 | `harness/src/tool_router.py` | `resolve()` 屏蔽本地/MCP 差异 + 超时保护 + 审计 |
| 权限沙箱 | `harness/src/permission.py` | 角色×级别矩阵 + 人工审批流 + 危险级硬拒绝 |
| 可观测 | Router.audit + SessionReport | 每次工具调用的 args/耗时/裁决记录 |

**权限模型详解**（面试最高频的安全题）：三维矩阵 = 用户角色（admin/engineer/viewer）× 工具级别（read/write/dangerous）→ 裁决（allow/deny/require_approval）。三条铁律：① **deny 不可被审批推翻**（危险级操作必须由平台管理员改造工具本身）；② **审批只对本会话生效**（防止一次批准终身放行）；③ **拒绝也要留痕**（审计日志记录被拦截的调用，用于回溯攻击面）。

### 2.3 常见 Harness 实现对照

| 实现 | 定位 | 借鉴点 |
|---|---|---|
| ChatGPT/Claude 后端 | 闭源商业 Harness | 系统提示注入、工具权限管理、安全过滤的层次划分 |
| ZCode/Cursor 等 IDE Agent | 开发域 Harness | 文件/终端权限沙箱、每次操作确认机制 |
| LangGraph Runtime | 开源编排引擎 | 状态图驱动的生命周期、checkpointer 持久化 |
| 企业自研 | 内部平台 | 认证集成、审计合规、成本管控（本项目的教学方向） |

## 3. Skill：可复用的领域能力包

### 3.1 Skill 的五大组成（对照 `skills/alarm-analysis/`）

```
skills/alarm-analysis/
├── skill.json    ① 元数据：名称/版本/触发词/权限/工具清单（可发现的根基）
└── skill.py      ②③④⑤ 工作流逻辑：确定性步骤 + 工具定义引用 + 提示模板 + I/O 契约
```

**Skill 的四个特征如何落地**：
- **自包含**：新增 Skill 只需新增目录，运行时零改动（SkillManager 自动发现）；
- **可发现**：触发词→匹配算法（教学版子串，生产版"向量初筛→规则加分→LLM 终裁"三级）；
- **可组合**：本项目的组合单元是工具调用；更强形态是 Skill 调 Skill（DAG 编排，模块 32）；
- **领域专业性**：alarm-analysis 内置"告警→诊断→报告"的领域工作流，调用者无需懂内部步骤。

### 3.2 Skill vs Tool：什么时候该封装成 Skill（面试必考）

| 维度 | Tool | Skill |
|---|---|---|
| 粒度 | 单次操作（查一次告警） | 一类任务的完整流程（告警分析=查+诊断+报告） |
| 谁来编排 | LLM 现场决定调用序列 | 领域专家预先设计工作流，LLM 只做开放性判断 |
| 确定性 | 取决于单次调用 | 高（关键判定用代码：本项目 config-audit 的基线比对） |
| 复用方 | Agent | Agent、API、定时任务、其他 Skill |

**封装判断标准**：① 流程稳定且可预先设计 → Skill；② 需要"每次都要 LLM 现场发挥"的探索 → 保持裸 Tool；③ 有合规审计要求的判定逻辑 → 必须沉淀为 Skill 中的代码规则（LLM 概率性输出在审计场景是负资产）。

### 3.3 Skill 的发现与路由设计（进阶）

当 Skill 数量到几十个，子串匹配失效。生产三级路由：
1. **向量初筛**：Skill 描述 + 触发词样本预先 Embedding，用户问题检索 top-5；
2. **规则加分**：命中触发词/正则的加权重排；
3. **LLM 终裁**：把 top-5 的名称+描述+边界示例给 LLM 选一个（或 none）——成本可控（只多一次小调用）且兜住语义理解。

版本管理与热更新：skill.json 的 version 语义化（破坏性变更升主版本）；运行时按目录 mtime/版本号热加载；灰度 = 新版本 Skill 按流量比例承接（对照模块 34）。

## 4. MCP：AI 应用的 USB-C

### 4.1 三层架构与协议面

```
┌─────────────┐    ┌──────────────┐    ┌─────────────────┐
│  MCP Host   │───►│  MCP Client  │◄──►│   MCP Server    │
│ (Harness/   │    │ (每 Server    │    │ tools/resources │
│  IDE/应用)   │ 1:1│  一个连接)    │    │ /prompts        │
└─────────────┘    └──────────────┘    └─────────────────┘
   本项目: agent_runtime      mcp_client.py        mcp_server.py
```

**协议握手**（手写实现后你会倒背如流）：Client → `initialize`（协议版本+自身能力）→ Server 应答（能力清单：tools/resources/prompts + serverInfo）→ Client 发 `notifications/initialized` → 正常调用（`tools/list` → `tools/call`）。

**三种能力的区别**（面试常考）：
- **Tools**：模型**主动调用**的函数（有副作用），如查询告警、下发规则；
- **Resources**：Host **拉取**的数据源（只读），如 SLA 阈值表、处置手册——适合"背景知识"而非"动作"；
- **Prompts**：Server 预置的**提示模板**，宿主按需取用（把领域话术标准化）。
判定口诀：要"做"用 Tool，要"看"用 Resource，要"说得好"用 Prompt。

### 4.2 传输层：stdio 与 Streamable HTTP

| | stdio | Streamable HTTP |
|---|---|---|
| 形态 | Server 是 Client 拉起的**子进程** | Server 独立进程/容器，POST /mcp |
| 隔离性 | 强（进程级，权限边界天然清晰） | 中（网络边界，需认证） |
| 适用 | 本机个人工具（IDE Agent 场景） | 团队共享/云端/跨机（本项目容器化形态） |

### 4.3 MCP vs 直接 Function Calling vs 传统 REST（对比实验的结论）

`examples/e2e_demo.py` 的实测对比表（摘要）：

| 维度 | 直接函数调用 | MCP 接入 |
|---|---|---|
| Skill/Agent 代码 | **零差异**（本实验实证） | 同左 |
| 接入成本 | import 即用 | 需实现/引入协议层，但一次投入长期复用 |
| 进程隔离 | 无 | 有（Server 崩溃不波及 Harness） |
| 动态发现 | 无（编译期固定） | 有（tools/list 运行时发现，热插拔） |
| 跨语言跨机 | 否 | 是 |
| 单次开销 | 函数调用 ~0ms | 本机 stdio 毫秒级（远程需算 RTT） |

**选型结论**：单机原型用函数直调；多团队/多语言/需独立部署与统一权限治理时上 MCP。**MCP 的核心价值不是"能调用"（FC 也能），而是"标准化的接入边界"**——工具提供方与消费方解耦，任何 MCP Host 都能接入任何 MCP Server。

### 4.4 MCP 安全（高级题）

- **能力声明复核**：Server 声明的工具级别，Host（Harness）要复核（最小权限原则：Host 的裁决优先于 Server 的声明）；
- **防替换攻击**：Server 被恶意替换后 tools/list 可能返回同名但行为不同的工具。防御：Server 指纹校验（首次发现时记录 serverInfo+工具 Schema 哈希，变更即告警）、签名分发（企业内 MCP Registry 统一签发）；
- **数据外泄**：工具参数可能携带敏感上下文，写操作工具（如"发送到外部"）必须走 require_approval；
- **Prompt 注入借道**：工具返回的内容进入上下文后可能包含注入指令——工具结果做净化/标记（untrusted 数据不直接当指令执行）。

## 5. 三者协作：一次完整的调用链路（端到端实证）

```
用户: "eNB-1024 的告警怎么回事，给出处理建议"
  │
  ▼
Harness/AgentRuntime.create_session("ops-user", role=engineer)        ← ①生命周期
  │ 意图匹配
  ▼
SkillManager.match → alarm-analysis（触发词"告警/建议"命中）            ← Skill 发现
  │ Skill 工作流
  ▼
router.resolve("query_alarms") → 本地函数 or MCP teleops-mcp-server    ← ③路由
  │ 权限裁决: engineer × read → allow                                  ← ④沙箱
  ▼
query_alarms(eNB-1024) → 2 条告警（小区退服+光模块异常）                  ← 工具执行
  │ 追问语义
  ▼
run_diagnosis(eNB-1024) → 光功率 -28.3dBm 低于阈值                      ← MCP tools/call
  │ 数据 + 结构化 Prompt
  ▼
LLM 综合判断 → Markdown 处置报告                                        ← ②上下文（含事实沉淀）
  │
  ▼
审计日志: [query_alarms, run_diagnosis] 全程留痕                        ← ⑥可观测
```

**面试一句话总结**：Harness 是舞台（约束与支撑），Skill 是剧本（领域工作流），MCP 是标准化的道具接口（外部系统接入）——三者叠加把"LLM 会聊天"变成"系统能运维"。

## 自检六问

1. Harness 六大职责分别对应本项目的哪个文件？（2.2 表）
2. 为什么 deny 级别不可被人工审批推翻？（2.2 铁律）
3. Skill 和 Tool 的封装判断标准是什么？（3.2）
4. MCP 的 initialize 握手交换了什么？（4.1）
5. Tools/Resources/Prompts 的判定口诀？（4.1）
6. 对比实验中"Skill 代码零差异"说明了什么？（4.3：Harness 抽象的价值）
