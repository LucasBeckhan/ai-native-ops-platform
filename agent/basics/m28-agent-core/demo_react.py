#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/basics/m28-agent-core/demo_react.py
# 所属模块: 模块 28 —— ReAct Agent 端到端演示（多工具串联 + 异常注入）
# 运行方式: python3 demo_react.py   （需 Mock LLM :18000）
# 说明:
#   - 演示一：多步推理——"告警→诊断→结论"的工具串联，展示 Agent 如何
#     把两个工具的观察拼接成一个结论（这是单次 Function Calling 做不到的）。
#   - 演示二：异常自愈——请求一个不存在的工具，观察 Agent 收到错误
#     Observation 后的自我修正（ReAct 优雅性的核心证据）。
#   - 演示三：长任务记忆——结合 LongTermMemory 检索历史案例作为上下文。
# =====================================================================
from react_agent import ReActAgent
from tools import ToolRegistry, Tool, query_alarms, query_kpis, run_diagnosis, build_default_registry
from memory import LongTermMemory
from llm_adapter import LLMAdapter


def main() -> None:
    print("========== 演示一：多步推理（告警 → 诊断 → 结论） ==========")
    agent = ReActAgent()
    result = agent.run("基站 eNB-1024 出了问题，帮我查一下原因并给出处理建议")
    print(f"步骤数: {result.steps_used} | 成本: ${result.total_cost_usd:.6f}")
    for t in result.trace:
        print(f"  [{t.step}] {t.kind}: {t.detail[:90]}")
    print(f"  最终答案: {result.answer[:150]}")

    print("\n========== 演示二：工具异常的自愈 ==========")
    # 注册一个会抛异常的故障工具，观察 Agent 把异常当观察并自我修正
    reg = build_default_registry()

    def broken_tool(**kwargs):
        raise RuntimeError("模拟下游网管接口超时")

    reg.register(Tool(
        name="query_topology", description="查询网络拓扑（演示用，会故障）",
        parameters={"type": "object", "properties": {}}, func=broken_tool,
    ))
    agent2 = ReActAgent(registry=reg)
    r2 = agent2.run("查一下 eNB-1024 的告警")
    for t in r2.trace:
        print(f"  [{t.step}] {t.kind}: {t.detail[:90]}")

    print("\n========== 演示三：长期记忆注入上下文 ==========")
    ltm = LongTermMemory()
    ltm.add("历史案例 case-0930：eNB-1024 曾因尾纤弯折导致光功率骤降，更换尾纤恢复。")
    ltm.add("历史案例 case-1012：eNB-1033 高温告警为空调故障，现场处理后消除。")
    hits = ltm.search("eNB-1024 光模块接收异常")
    print(f"  检索到相似历史案例: {hits[0]['text'][:60]} (score={hits[0]['score']})")
    print("  => 生产实现：检索结果注入系统提示或用户消息，让 Agent 引用历史经验")


if __name__ == "__main__":
    main()
