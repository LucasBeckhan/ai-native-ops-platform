#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/basics/m27-llm-adapter/demo_adapter.py
# 所属模块: 模块 27 —— 统一适配层演示（五个生产场景）
# 运行方式: python3 demo_adapter.py    （需 Mock LLM 在 localhost:18000）
# 说明:
#   - 五个场景对应生产上五个真实问题：
#     ① 基本调用与用量观测  ② 主备降级（厂商故障时的连续性）
#     ③ 流式输出（交互体验）④ 结构化输出修复（真实模型输出不总是纯 JSON）
#     ⑤ 成本核算（预算与告警的数据来源）
#   - 全部对 Mock LLM 运行，零 API Key；切真实厂商见 docs 文末说明。
# =====================================================================
from llm_adapter import (
    ChatMessage, Role, LLMAdapter, ProviderConfig, Provider,
    DEFAULT_PROVIDERS, extract_json, Usage,
)


def main() -> None:
    print("=" * 60)
    print("场景 1：基本调用——用量、延迟、成本三要素")
    adapter = LLMAdapter()
    r = adapter.chat([ChatMessage(Role.USER, "查询基站 eNB-7 的告警")])
    print(f"   模型={r.model} 用量={r.usage.total}tok 延迟={r.latency_ms}ms 成本=${r.cost_usd:.6f}")

    print("=" * 60)
    print("场景 2：主备降级——主槽位端口不可达，备槽位自动接管")
    fallback = LLMAdapter(
        providers={
            "primary": ProviderConfig(Provider.OPENAI, "http://localhost:19999/v1", "X", "gpt-4o-mini"),
            "backup": DEFAULT_PROVIDERS["mock"],
        },
        max_retries=1, backoff_base_s=0.1,
    )
    r2 = fallback.chat([ChatMessage(Role.USER, "ping")])
    print(f"   最终由 {r2.model} 服务（日志可见 primary 失败→切换过程）")

    print("=" * 60)
    print("场景 3：流式输出——增量回调实现打字机效果")
    pieces: list[str] = []
    r3 = adapter.chat(
        [ChatMessage(Role.USER, "用三句话解释 Kafka 的 ISR 机制")],
        stream_callback=lambda p: pieces.append(p),
    )
    print(f"   收到 {len(pieces)} 个增量片段，聚合 {len(r3.content)} 字符（首个片段: {pieces[0]!r}）")

    print("=" * 60)
    print("场景 4：结构化输出修复——真实模型常见'围栏包裹'的清洗")
    messy = '好的，结果如下：\n```json\n{"severity": "critical", "impact": "小区退服"}\n```\n请查收。'
    parsed = extract_json(messy)
    print(f"   原始输出含 markdown 围栏，清洗后: {parsed}")

    print("=" * 60)
    print("场景 5：成本核算——价目表换算与预算控制的数据基础")
    usage = Usage(prompt_tokens=1_000_000, completion_tokens=500_000)
    print(f"   gpt-4o-mini 100万入+50万出 = ${LLMAdapter._cost_of('gpt-4o-mini', usage):.2f}")
    print("   => 模块 33 的成本大盘按此口径聚合 adapter 的 on_usage 回调")


if __name__ == "__main__":
    main()
