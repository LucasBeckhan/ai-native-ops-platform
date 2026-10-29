#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/basics/m28-agent-core/react_agent.py
# 所属模块: 模块 28 —— 从零实现 ReAct Agent（推理-行动-观察循环）
# 运行方式: 作为库被 demo_react.py 引用；单独运行自检一轮循环
# 说明:
#   - ReAct 循环的完整生命周期（每段注释对应一个工程决策）：
#       用户问题 → Thought(规划) → Action(选工具+填参数) → Observation(执行)
#           → 循环直到 LLM 认为信息足够 → Final Answer
#   - 三个必须处理的生产问题：
#     ① 循环上限（max_steps）：LLM 可能陷入重复调用，必须强制作答；
#     ② 工具执行异常：异常作为 Observation 返回给 LLM 而不是崩溃——
#        让 Agent 自己决定换路还是放弃（这是 ReAct 优雅性的来源）；
#     ③ 参数校验：LLM 给的参数不可信，执行前必须校验（缺参/类型错），
#        校验失败把错误信息喂回 LLM 让它修正——自我修复闭环。
#   - 实现基于模块 27 适配层 + OpenAI Function Calling 协议；
#     "Thought" 在 FC 模式下由模型内部完成，trace 中以步骤记录呈现。
# =====================================================================
from __future__ import annotations

import json
import sys
import os
from dataclasses import dataclass, field

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../m27-llm-adapter"))

from llm_adapter import ChatMessage, Role, LLMAdapter  # noqa: E402
from tools import ToolRegistry, build_default_registry  # noqa: E402
from memory import ShortTermMemory, Scratchpad  # noqa: E402


@dataclass
class StepTrace:
    """一步执行的完整记录（模块 33 可观测性的数据来源）。"""
    step: int
    kind: str            # thought / action / observation / final / error
    detail: str


@dataclass
class ReActResult:
    """一次 Agent 运行的最终产物：答案 + 全程轨迹 + 成本。"""
    answer: str
    trace: list[StepTrace] = field(default_factory=list)
    steps_used: int = 0
    total_cost_usd: float = 0.0


class ReActAgent:
    """最小可用的 ReAct Agent：规划-执行-观察-收敛。"""

    def __init__(
        self,
        llm: LLMAdapter | None = None,
        registry: ToolRegistry | None = None,
        system_prompt: str = "",
        max_steps: int = 6,
        temperature: float = 0.2,
    ):
        self.llm = llm or LLMAdapter()
        self.registry = registry or build_default_registry()
        self.max_steps = max_steps
        self.temperature = temperature
        # 系统提示：角色 + 工具使用纪律 + 终止条件（ReAct 的"舞台说明"）
        self.system_prompt = system_prompt or (
            "你是通信网络运维助手。你可以调用工具获取真实数据。\n"
            "纪律：\n"
            "1. 回答必须基于工具返回的数据，禁止编造设备/告警/指标；\n"
            "2. 每次只调用一个工具，等待结果后再决定下一步；\n"
            "3. 信息足够时立即作答，不要重复查询；\n"
            "4. 数据不足时如实说明，并指出还缺什么信息。"
        )

    # ------------------------------------------------------------------
    def run(self, user_query: str) -> ReActResult:
        """执行一次完整的 ReAct 循环。"""
        stm = ShortTermMemory(max_tokens=3000)
        stm.add("system", self.system_prompt)
        stm.add("user", user_query)
        trace: list[StepTrace] = []
        scratch = Scratchpad()
        total_cost = 0.0

        for step in range(1, self.max_steps + 1):
            # ---- 步骤 1：携带工具声明请求 LLM（Thought + Action 由模型完成）----
            resp = self.llm.chat(
                messages=[ChatMessage(Role(m["role"]), m["content"], name=m.get("name"))
                          for m in stm.as_openai_messages()],
                tools=self.registry.openai_schemas(),
                temperature=self.temperature,
            )
            total_cost += resp.cost_usd

            if resp.tool_calls:
                # ---- 步骤 2：Action——执行模型选择的工具 ----
                call = resp.tool_calls[0]
                fname = call["function"]["name"]
                try:
                    args = json.loads(call["function"].get("arguments") or "{}")
                except json.JSONDecodeError:
                    args = {}
                trace.append(StepTrace(step, "action", f"{fname}({args})"))
                scratch.note("Action", f"{fname}({args})")

                observation = self._execute_tool(fname, args)
                trace.append(StepTrace(step, "observation", observation))
                scratch.note("Observation", observation)

                # ---- 步骤 3：把 assistant(工具调用) 与 tool(结果) 回填上下文 ----
                stm.add("assistant", json.dumps({"tool_call": call}, ensure_ascii=False))
                stm.add("tool", observation, name=fname)
                continue

            # ---- 步骤 4：Final Answer——模型不再调工具，输出收敛 ----
            answer = resp.content or "（模型未返回内容）"
            trace.append(StepTrace(step, "final", answer))
            return ReActResult(answer=answer, trace=trace,
                               steps_used=step, total_cost_usd=total_cost)

        # ---- 步骤 5：循环上限兜底——强制作答，防止死循环烧钱 ----
        forced = (
            "已达到工具调用步数上限。基于已收集的信息直接给出结论；"
            "信息不足时明确说明缺口。已收集信息：\n" + scratch.render()
        )
        stm.add("user", forced)
        resp = self.llm.chat(messages=[ChatMessage(Role(m["role"]), m["content"])
                                       for m in stm.as_openai_messages()],
                             temperature=self.temperature)
        trace.append(StepTrace(self.max_steps, "final", resp.content or ""))
        return ReActResult(answer=resp.content or "", trace=trace,
                           steps_used=self.max_steps, total_cost_usd=total_cost)

    # ------------------------------------------------------------------
    def _execute_tool(self, name: str, args: dict) -> str:
        """执行工具并统一把结果/异常转为 Observation 字符串。
        任何异常都不向上抛——它们是 Agent 决策的输入，不是程序故障。"""
        try:
            tool = self.registry.get(name)
        except KeyError as e:
            return f"工具不存在: {e}"
        # 参数校验：required 缺失时把问题喂回 LLM（自我修复闭环）
        required = tool.parameters.get("required", [])
        missing = [k for k in required if k not in args]
        if missing:
            return f"参数校验失败：缺少必需参数 {missing}。请修正参数后重新调用。"
        try:
            result = tool.func(**args)
        except TypeError as e:
            return f"参数类型错误: {e}。请检查参数名与类型后重试。"
        except Exception as e:                     # noqa: BLE001 —— 见函数头注释
            return f"工具执行异常: {type(e).__name__}: {e}"
        return json.dumps(result, ensure_ascii=False)


if __name__ == "__main__":
    # 自检：不启动服务的最小循环（Mock LLM 需在 18000 端口）
    agent = ReActAgent()
    result = agent.run("查一下基站 eNB-1024 有什么告警，严重吗？")
    print(f"步骤数: {result.steps_used}，成本: ${result.total_cost_usd:.6f}")
    for t in result.trace:
        print(f"  [{t.step}] {t.kind}: {t.detail[:80]}")
    print("答案:", result.answer[:120])
