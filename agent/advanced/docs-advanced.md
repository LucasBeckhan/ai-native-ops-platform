# Agent 进阶（模块 30-34）知识文档

> **配套代码**：`agent/advanced/multi-agent/ops_agents.py`（模块 32，已实测）；RAG 复用 `agent/basics/m28-agent-core/memory.py`；其余模块文档为主+关键代码
> **预估学习时间**：模块 30-34 共 18 小时 | **优先级**：P1

---

## 模块 30：RAG 系统深度构建

### 30.1 五段流水线与本项目实现

```
文档加载 → 分块 → 向量化 → 检索 → 生成
              │                    │
        递归/语义/Agentic      向量库（本项目 in-process）
        三种分块对比           Milvus(生产)/Chroma(轻量)/memory.py(教学)
```

- **分块策略对比**：递归分块（按段落+固定长度，简单通用）；语义分块（按 embedding 相似度切分主题，成本高）；Agentic 分块（LLM 决定切点，最贵最准）。运维手册类结构化文档，**按章节+条目递归分块**通常最优。
- **检索升级**：Dense（向量）+Sparse（BM25 关键词）混合召回 → Reranker 精排（交叉编码器）；查询改写（口语→术语）；HyDE（先让 LLM 生成假想答案再检索）。
- **评估**：RAGAS 四指标（忠实度/答案相关性/上下文精确率/上下文召回率）——没有评估的 RAG 调优是玄学。
- **本项目实现**：`memory.py` 的 LongTermMemory 就是 RAG 的最小骨架（确定性伪向量+余弦检索），接口设计与 Chroma 对齐；上生产换 Milvus（`docker-compose.milvus.yml --profile milvus`）只改实现不改调用方。

### 30.2 通信运维知识库的特殊性

设备手册/故障案例/配置基线三类语料的分块策略差异大：手册按章节、案例按"现象-原因-处置"三元组、基线按规则条目——**元数据设计（设备型号/版本）决定过滤检索质量**。

## 模块 31：Tool/Function Calling 深度

- **工具编排**：依赖管理（先查后改）、并行执行（无依赖工具并发调用）、失败容错（重试→换路→降级）；
- **MCP 深度**（模块 29 的延伸）：Client 连接池（多 Server 常驻）、健康检查与自动重连、权限协商（Host 复核 Server 声明）、多 Server 工具名冲突（前缀命名空间，本项目 router 已实现）；
- **通信场景工具**：设备命令执行（白名单+审批）、配置查询、告警检索（本项目 es_query_alerts）、流量分析——四类工具的权限分级见场景二。

## 模块 32：多 Agent 协作（★ 已实测）

### 32.1 三种协作拓扑

| 拓扑 | 结构 | 适用 |
|---|---|---|
| 流水线接力 | A→B→C 固定顺序 | 流程稳定的处置链（本项目监控→诊断→工单） |
| 中心调度 | Manager 拆解分发，Worker 执行 | 可并行子任务（全网巡检分片） |
| 群体辩论 | 多 Agent 独立判断+聚合 | 高风险决策（变更评审） |

### 32.2 本项目实现的三条原则（ops_agents.py）

1. **结构化事件单传递**：Monitor 产出 `EVT-xxx` JSON（含告警证据），而不是共享对话历史——信息按需最小化；
2. **代码兜底**：DiagnoseAgent 的 LLM JSON 解析失败时，从诊断数据用规则直接推导根因（实测命中"光功率 -28.3dBm"置信度 0.9）——结构化数据的确定性优先，LLM 是叙述层；
3. **权限分体**：monitor=viewer、diagnose=engineer、ticket=engineer，Router 统一审计（多 Agent 无审计断链）。

### 32.3 冲突解决

两个 Agent 同时处置同一设备 → Harness 层加锁（会话级互斥）；结论冲突（诊断 vs 监控）→ 置信度仲裁 + 人工升级。

## 模块 33：可观测性与安全

- **链路追踪**：每次 LLM 调用（模型/Token/延迟）与工具调用（本项目 StepTrace/Router.audit）落 trace，LangFuse 自建或 LangSmith SaaS；
- **行为评估**：任务成功率/步数/成本/越权率四指标（模块 28 Q7 的评估框架）；
- **安全防线**（对照模块 29 Q7）：Prompt 层（反编造约束）→ 参数层（Schema+值域校验）→ 权限层（Harness 矩阵）→ 执行层（白名单+审计）→ 输出层（敏感信息脱敏）；
- **Prompt Injection 防御**：工具返回内容标记 untrusted（不当指令执行）、外发类工具强制审批、输入输出双向过滤。

## 模块 34：部署与运维

- **模型服务化**：vLLM（吞吐王，PagedAttention/连续批处理）vs Ollama（个人开发）vs TGI；OpenAI 兼容端点让适配层零改动切换（模块 27）；
- **Agent 服务化**：本项目 `platform/agent-service`（FastAPI 包装 Harness）+ `docker-compose.agent.yml`（实测通过）→ 模块 34 的 K8s 部署 = Helm 包一层（对照场景一 deploy/）；
- **Harness 生产化**：配置中心接入（阈值/模型路由热更）、Skill 热加载（skill.json 版本监听）、MCP Server 动态注册（Registry webhook）；
- **灰度发布**：新旧 Skill/模型按流量分流，业务指标（采纳率）回看后全量。
