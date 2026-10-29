# Agent 方向面试题汇总

> 完整解析（考察意图/加分回答/误区）分布在各模块文件中；本文件是索引 + 高频速答补充。

## 一、★ Skill / MCP / Harness 专题（11 道，全带完整解析）

**文件**：`agent/harness/docs/interview-m29.md`

1. Harness/Skill/MCP 三者定位区别与协作场景
2. 企业 Agent 平台的 Harness 层架构设计
3. Skill 和 Tool 的区别与封装时机
4. MCP vs Function Calling 的额外价值
5. Tools/Resources/Prompts 三能力区别
6. Skill 发现与路由机制（几十个 Skill 如何选）
7. Harness 权限控制方案（危险操作防护）
8. MCP 安全与 Server 恶意替换检测
9. Skill 版本管理与热更新
10. MCP vs 传统 REST 集成对比
11. 多 Agent 场景的上下文隔离与共享

## 二、模块面试题索引（全带完整解析）

| 文件 | 题数 | 覆盖 |
|---|---|---|
| agent/basics/m26-prompts/docs/interview-m26.md | 6 | Transformer/Token/Prompt 方法论/幻觉/资产管理 |
| agent/basics/m27-llm-adapter/docs/interview-m27.md | 6 | 协议差异/重试熔断降级/流式/Key 安全/成本/路由 |
| agent/basics/m28-agent-core/docs/interview-m28.md | 7 | ReAct 手写/FC 协议/Memory/工作流 vs Agent/参数安全/评估 |
| agent/advanced/multi-agent/docs-interview-advanced.md | 8 | RAG 排障/多 Agent 共享/JSON 兜底/灰度/成本/vLLM/注入/多 Agent 设计 |
| agent/harness/docs/interview-m29.md | 11 | 专题（见上） |

## 三、高频速答补充（8 道）

**Q1 Agent 与传统工作流的本质区别？**
要点：路径确定性（工作流固定/Agent 动态）、错误代价、成本（Agent 每步一次 LLM 调用）。判断树见模块 28 Q4；能写死的不交给 Agent。

**Q2 Function Calling 的参数是模型生成的，如何保证安全？**
要点：四道防线——Schema 校验、业务值域校验、权限矩阵（read/write/dangerous）、注入防御（参数化/白名单）。落点见 react_agent 的校验回喂与 permission.py 矩阵。

**Q3 Agent 的记忆为什么会"越聊越乱"？怎么治理？**
要点：上下文塞满历史→注意力稀释+成本平方级；治理=分层记忆（保头保尾裁剪/长期向量化/工作草稿）+写入门槛（只沉淀总结/纠正/模式）。

**Q4 多轮对话上下文超限怎么办？**
要点：裁剪（保头保尾）→ 摘要压缩（旧历史 LLM 总结成 facts）→ 外置存储按需检索（记忆即 RAG）。本项目 ContextManager 实现前两者。

**Q5 如何评估一个 RAG 系统的好坏？**
要点：RAGAS 四指标（忠实度/答案相关性/上下文精确率/召回率）+ 业务指标（采纳率）；评估集 50+ 条定期回归；分层归因（检索没召回 vs 召回没答好）。

**Q6 什么是 Agent 的"幻觉"在工具链上的特殊形态？**
要点：编造工具参数（不存在的设备号）、编造工具结果（未调用却声称查过）、引用幻觉（结论无证据）。防御：数据驱动回答（先工具后结论）+ 引用证据编号 + 输出护栏。

**Q7 Skill 热更新时在途请求怎么办？**
要点：新旧版本并存（N-1/N），在途请求继续用旧版，新请求按灰度比例分流；状态迁移兼容或排空等待。参考模块 29 Q9。

**Q8 LLM 网关（Gateway）解决什么问题？**
要点：统一持钥与配额、多厂商路由与降级、审计计量、缓存——把 Key 安全/成本/路由从业务代码收敛到平台层。与 Harness 的边界：网关管"模型调用"，Harness 管"Agent 运行时"。
