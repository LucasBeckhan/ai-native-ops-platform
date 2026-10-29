# 模块 27：LLM API 集成与工程化

> **前置依赖**：模块 26
> **预估学习时间**：5 小时
> **优先级**：P0

## 学习目标

1. 说清三家协议五处核心差异，理解"OpenAI 兼容生态"的行业格局；
2. 实现重试（指数退避）/降级（主备槽位）双层的失败治理，区分三者边界；
3. 实现 SSE 流式输出的增量回调接口，理解网关缓冲等工程坑；
4. 建立 API Key 安全规范与成本计量-归因-预算-优化四步链路；
5. 理解 Mock Server 在 CI/测试/故障演练中的四个价值。

## 文件清单

| 文件 | 说明 |
|---|---|
| [docs/01-llm-adapter.md](docs/01-llm-adapter.md) | 知识文档：协议差异/失败治理/流式/安全/成本 |
| [docs/interview-m27.md](docs/interview-m27.md) | 面试题 6 道 |
| [labs/lab-27.md](labs/lab-27.md) | 实验指南：五场景 + 限流退避观测 + 成本回调 |
| [llm_adapter.py](llm_adapter.py) | ★ 统一适配层（零 SDK 依赖，含系统代理绕过策略） |
| [demo_adapter.py](demo_adapter.py) | 五个生产场景演示（全部实测） |
| ../mock-llm/ | Mock LLM Server（模块 27 讲解其设计价值） |

## 快速启动

```bash
cd docker && docker compose -f docker-compose.llm.yml up -d   # Mock LLM :18000
cd ../agent/basics/m27-llm-adapter
python3 demo_adapter.py
```

## 切换真实厂商（改配置不改代码）

```python
from llm_adapter import LLMAdapter, DEFAULT_PROVIDERS
adapter = LLMAdapter(providers={
    "primary": DEFAULT_PROVIDERS["qwen"],     # export DASHSCOPE_API_KEY=...
    "backup":  DEFAULT_PROVIDERS["mock"],
})
```
