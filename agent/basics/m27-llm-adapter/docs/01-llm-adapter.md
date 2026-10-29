# 模块 27：LLM API 集成与工程化

> **前置依赖**：模块 26（Prompt 基础）；Mock LLM 已启动
> **预估学习时间**：5 小时（文档 1.5h + 实验 2.5h + 面试题 1h）
> **配套代码**：`agent/basics/m27-llm-adapter/{llm_adapter.py, demo_adapter.py}`
> **配套实验**：`labs/lab-27.md`

---

## 1. 为什么需要统一适配层

直接在业务代码里 `import openai` 的三个代价：

1. **厂商锁定**：换模型 = 全代码库改调用；OpenAI / Claude / 通义千问的请求响应结构各不相同；
2. **失败裸奔**：网络抖动、429 限流、厂商故障直接把异常抛给用户；
3. **成本黑箱**：没人说得清一条告警分析花了几分钱、一个月烧多少。

统一适配层把这三件事收敛到一个类里（见 `llm_adapter.py`，约 260 行，零 SDK 依赖）：

```
业务代码 ──ChatMessage──► LLMAdapter ──┬─► OpenAI 协议（Mock/DeepSeek/vLLM/Qwen兼容层）
        ◄──ChatResponse──────────────┼─► Claude Messages 协议
                                     └─► 重试/降级/流式/成本核算
```

## 2. 三家协议差异速查（面试可直接引用）

| 维度 | OpenAI（含兼容层） | Claude | 通义千问 |
|---|---|---|---|
| 端点 | /v1/chat/completions | /v1/messages | /compatible-mode/v1/chat/completions |
| 认证 | `Authorization: Bearer` | `x-api-key` + `anthropic-version` | Bearer |
| 系统提示 | messages 里的 system 角色 | 顶层 `system` 参数 | 同 OpenAI |
| 工具调用 | message.tool_calls 数组 | content 块里的 `tool_use` | 同 OpenAI |
| 工具结果 | `role:"tool"` + tool_call_id | content 块 `tool_result` | 同 OpenAI |
| 流式 | SSE，`choices[].delta` | SSE，`content_block_delta` 事件 | 同 OpenAI |
| Token 统计 | usage.prompt/completion_tokens | usage.input/output_tokens | 同 OpenAI |

**工程结论**：千问的"OpenAI 兼容模式"意味着接入它几乎零成本；Claude 的差异需要真实转换代码（本适配层已实现映射）。这也是"优先选 OpenAI 兼容生态"成为行业默认的原因。

## 3. 失败治理：重试、熔断、降级的边界

```
请求 ──► [429/5xx?] ──是──► 指数退避重试（0.5s→1s→2s，上限 max_retries）
      │                        │ 重试耗尽
      └──[4xx 参数错]──否──────┼──► 抛 LLMError
                               ▼
                     切换下一个 provider 槽位（主→备）
                               │ 全部槽位失败
                               ▼
                        抛出统一 LLMError
```

三条设计决策（面试常追问"为什么不只做重试"）：

1. **重试只对 429/5xx/网络错**：4xx 是调用方参数错误，重试只是放大故障；
2. **重试与降级分层**：重试解决"瞬时抖动"，降级解决"单厂商持续故障"——两层都要；
3. **指数退避 + 抖动**（本实现未加抖动，生产建议 `wait = base * 2^n + random(0, base)`）：防止重试风暴在同一时刻齐发。

**熔断**（本实现保留接口）：连续 N 次失败后直接跳过该槽位一段时间（半开探测恢复）。教学版未实现完整状态机，模块 33 的生产化讨论会补全。

## 4. 流式输出（SSE）的工程要点

- **协议**：`Content-Type: text/event-stream`，服务端逐块推送 `data: {...}`，以 `data: [DONE]` 结束；
- **客户端接口设计**：适配层暴露"增量回调"（`stream_callback(piece)`），UI 层用它做打字机；聚合结果照常返回 `ChatResponse`——**流式只改变传输方式，不改变编程模型**；
- **坑位清单**：① 代理/网关会缓冲 SSE 导致"假流式"（本机实验就是被系统代理缓冲的实例，见适配层 `_client_for` 的直连策略）；② usage 在最后一个 chunk（需 `stream_options.include_usage`）；③ 工具调用流的参数是分片 JSON，需要拼接后再解析。

## 5. API Key 安全与多模型路由

- **Key 管理**：只从环境变量/密钥管理系统读取（适配层的 `api_key_env` 设计），禁止入库入仓；生产用 Vault/KMS + 定期轮换；日志中必须脱敏（`sk-***`）。
- **多模型路由的三种策略**：① 按槽位主备（本实现，可用性优先）；② 按任务路由（简单任务→小模型省钱，复杂任务→大模型）；③ 按负载/成本动态路由（综合 SLA 与价目表打分）。路由的评估基础是模块 26 的"评估集"——没有评估集的路由是盲选。

## 6. Mock Server 的设计价值（对应 mock-llm/）

生产团队自建 Mock LLM 的四个理由：① CI 无密钥可跑全链路；② 输出确定性 → 集成测试可断言；③ 注入故障（延迟/错误率）演练降级链路；④ 成本为零。本项目 Mock 实现（`agent/basics/mock-llm/mock_llm_server.py`）覆盖 OpenAI 协议的 chat/工具调用/流式/embeddings 四类接口，并暴露 Prometheus 指标。

## 自检四问

1. 为什么 4xx 不重试而 429 要重试？（第 3 节）
2. Claude 的工具调用在响应结构上与 OpenAI 有何不同？（第 2 节表格）
3. 流式输出如何做到"不改变上层编程模型"？（第 4 节增量回调+聚合返回）
4. "按任务路由模型"的前提是什么？（第 5 节：有评估集）
