#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/harness/src/agent_runtime.py
# 所属模块: 模块 29 —— Harness 组件四：Agent 运行时（生命周期 + 对话循环）
# 运行方式: 作为库被 examples/e2e_demo.py 引用；可独立自检
# 说明:
#   - 运行时 = Harness 的总装车间，串起另外三个组件：
#       ContextManager（每轮上下文）→ LLMAdapter（模型调用）→
#       ToolRouter（工具执行，内含权限裁决）→ 审计日志。
#   - 会话生命周期：create → chat×N → pause/resume → close（模块 29 文档
#     "六大职责"中生命周期管理的最小实现）。
#   - 对话循环复用模块 28 的 ReAct 逻辑，但**工具执行必须经由 ToolRouter**
#     （权限与审计的强制路径）——这就是"Harness 是 Agent 的舞台"的含义：
#     Agent 的每次行动都在舞台的灯光（审计）与围栏（权限）之内。
#   - Skill 集成：chat() 前先由 SkillManager 做意图匹配，命中 Skill 则
#     执行 Skill 的工作流（Skill 内部同样经由 router 取数）。
# =====================================================================
from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../basics/m27-llm-adapter"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../basics/m28-agent-core"))

from llm_adapter import ChatMessage, Role, LLMAdapter  # noqa: E402
from context_manager import ContextManager, UserPreferences  # noqa: E402
from permission import PermissionManager  # noqa: E402
from tool_router import new_router  # noqa: E402


@dataclass
class SessionReport:
    """一次会话的运行报告（审计与成本归因的输出契约）。"""
    session_id: str
    turns: int = 0
    tool_calls: int = 0
    denied_calls: int = 0
    cost_usd: float = 0.0
    audit: list = field(default_factory=list)


class AgentRuntime:
    """多会话 Agent 运行时（教学版单线程；生产版见 docs 的扩展讨论）。"""

    def __init__(self, llm: LLMAdapter | None = None, use_mcp: bool = False,
                 mcp_command: list[str] | None = None, mcp_http_url: str | None = None,
                 skills_dir: str | None = None):
        self.llm = llm or LLMAdapter()
        self.contexts = ContextManager()
        self.router = new_router(use_mcp=use_mcp, mcp_command=mcp_command,
                                 mcp_http_url=mcp_http_url)
        self.permissions: dict[str, PermissionManager] = {}
        self.reports: dict[str, SessionReport] = {}
        self._base_system = (
            "你是通信网络运维助手。回答必须基于工具返回的真实数据，"
            "禁止编造设备/告警/指标。信息不足时如实说明。"
        )
        # Skill 管理：加载 skills/ 目录下的全部 Skill 元数据
        self.skills = []
        if skills_dir:
            from skill_manager import SkillManager
            self.skill_manager = SkillManager(skills_dir)
            self.skills = self.skill_manager.list_skills()
        else:
            self.skill_manager = None

    # ---------------- 生命周期管理 ----------------
    def create_session(self, session_id: str, user_id: str,
                       role: str = "engineer") -> None:
        self.contexts.create(session_id, user_id, self._base_system,
                             UserPreferences(role=role))
        self.permissions[session_id] = PermissionManager(
            session_id=session_id, user_id=user_id, role=role)
        self.reports[session_id] = SessionReport(session_id=session_id)

    def pause_session(self, session_id: str) -> None:
        self.contexts.pause(session_id)

    def resume_session(self, session_id: str) -> None:
        self.contexts.resume(session_id)

    def close_session(self, session_id: str) -> SessionReport:
        report = self.reports[session_id]
        self.contexts.close(session_id)
        return report

    # ---------------- 对话主入口 ----------------
    def chat(self, session_id: str, message: str, max_steps: int = 5) -> str:
        """一轮用户输入的完整处理：Skill 匹配 → ReAct 循环 → 报告累计。"""
        ctx = self.contexts.get(session_id)
        if ctx.status != "active":
            return f"[会话 {session_id} 处于 {ctx.status} 状态，无法响应]"
        perm = self.permissions[session_id]

        # ---- Skill 意图匹配：命中则走 Skill 工作流（领域封装的入口）----
        if self.skill_manager:
            skill = self.skill_manager.match(message)
            if skill:
                self.contexts.add_fact(session_id, "命中Skill", skill.name)
                answer = self.skill_manager.execute(
                    skill, session_id, message,
                    router=self.router, llm=self.llm, perm=perm)
                self.contexts.append(session_id, "user", message)
                self.contexts.append(session_id, "assistant", answer)
                self.reports[session_id].turns += 1
                return answer

        # ---- 通用 ReAct 循环（未命中 Skill 的兜底路径）----
        self.contexts.append(session_id, "user", message)
        final = ""
        for step in range(1, max_steps + 1):
            messages = self.contexts.build_messages(session_id)
            resp = self.llm.chat(
                messages=[ChatMessage(Role(m["role"]), m["content"], name=m.get("name"))
                          for m in messages],
                tools=self.router.openai_schemas(),
                temperature=0.2,
            )
            self.reports[session_id].cost_usd += resp.cost_usd

            if resp.tool_calls:
                call = resp.tool_calls[0]
                fname = call["function"]["name"]
                try:
                    args = json.loads(call["function"].get("arguments") or "{}")
                except json.JSONDecodeError:
                    args = {}
                # ★ 工具执行强制经由 Router（权限+审计的唯一通道）
                observation = self.router.execute(fname, args, perm)
                rep = self.reports[session_id]
                rep.tool_calls += 1
                if observation.startswith("[权限拒绝]") or observation.startswith("[需人工审批]"):
                    rep.denied_calls += 1
                self.contexts.append(session_id, "assistant",
                                     json.dumps({"tool_call": call}, ensure_ascii=False))
                self.contexts.append(session_id, "tool", observation, name=fname)
                continue

            final = resp.content or "（无内容）"
            break
        else:
            final = "已达本轮工具调用上限，请基于以上信息人工判断或缩小问题范围。"

        self.contexts.append(session_id, "assistant", final)
        self.reports[session_id].turns += 1
        return final


if __name__ == "__main__":
    # 自检：完整生命周期 + 权限联动的最小流程
    rt = AgentRuntime()
    rt.create_session("s-demo", "alice", role="engineer")
    print("活跃会话:", rt.contexts.active_sessions)
    ans = rt.chat("s-demo", "查一下 eNB-1024 有什么告警")
    print("回答(前80字):", ans[:80])
    rt.pause_session("s-demo")
    print("暂停后提问:", rt.chat("s-demo", "还在吗")[:50])
    rt.resume_session("s-demo")
    rt.close_session("s-demo")
    print("会话报告:", rt.reports["s-demo"])
