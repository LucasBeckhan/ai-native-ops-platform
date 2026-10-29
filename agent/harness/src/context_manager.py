#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/harness/src/context_manager.py
# 所属模块: 模块 29 —— Harness 组件一：上下文管理（Context Management）
# 运行方式: 作为库被 agent_runtime.py 引用；单独运行可自检
# 说明:
#   - Harness 六大职责之一。管理维度：
#     ① 会话生命周期内的对话历史（复用模块 28 的 ShortTermMemory）；
#     ② 系统提示与用户偏好（同一 Agent 对不同用户可以不同口径）；
#     ③ 上下文预算与压缩（成本控制的核心杠杆）。
#   - 与"裸用 ShortTermMemory"的区别：ContextManager 是**按会话组织**的
#     管理器（多用户/多会话并发），并负责把"系统提示+偏好+历史"拼装成
#     每轮请求的完整上下文——Agent 代码不再关心拼装细节。
# =====================================================================
import sys
import os
from dataclasses import dataclass, field

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../basics/m28-agent-core"))
from memory import ShortTermMemory  # noqa: E402


@dataclass
class UserPreferences:
    """用户级偏好：影响 Agent 的表达与严格程度。"""
    role: str = "engineer"            # admin / engineer / viewer（权限联动）
    language: str = "zh"
    verbose: bool = True              # 是否输出推理过程


@dataclass
class SessionContext:
    """一个会话的全部状态。"""
    session_id: str
    user_id: str
    prefs: UserPreferences
    system_prompt: str
    memory: ShortTermMemory = field(default_factory=lambda: ShortTermMemory(max_tokens=2500))
    # 会话级附加信息（Agent 执行中沉淀的中间事实，如"当前分析设备=eNB-1024"）
    facts: dict = field(default_factory=dict)
    status: str = "active"            # active / paused / closed


class ContextManager:
    """多会话上下文管理器：create / append / build_messages / compact。"""

    def __init__(self, token_budget: int = 2500):
        self.token_budget = token_budget
        self._sessions: dict[str, SessionContext] = {}

    def create(self, session_id: str, user_id: str, system_prompt: str,
               prefs: UserPreferences | None = None) -> SessionContext:
        if session_id in self._sessions:
            raise ValueError(f"会话已存在: {session_id}")
        ctx = SessionContext(
            session_id=session_id, user_id=user_id,
            prefs=prefs or UserPreferences(),
            system_prompt=system_prompt,
            memory=ShortTermMemory(max_tokens=self.token_budget),
        )
        ctx.memory.add("system", system_prompt)
        self._sessions[session_id] = ctx
        return ctx

    def get(self, session_id: str) -> SessionContext:
        if session_id not in self._sessions:
            raise KeyError(f"会话不存在: {session_id}")
        return self._sessions[session_id]

    def append(self, session_id: str, role: str, content: str, name: str | None = None) -> None:
        ctx = self.get(session_id)
        ctx.memory.add(role, content, name=name)

    def add_fact(self, session_id: str, key: str, value: str) -> None:
        """沉淀会话级事实（跨轮次可用的中间结论）。"""
        self.get(session_id).facts[key] = value

    def build_messages(self, session_id: str) -> list[dict]:
        """拼装本轮请求的完整上下文：system + 已沉淀事实 + 对话历史。"""
        ctx = self.get(session_id)
        msgs = ctx.memory.as_openai_messages()
        if ctx.facts:
            # 事实以 system 附注形式注入（比塞进用户消息更稳）
            facts_text = "; ".join(f"{k}={v}" for k, v in ctx.facts.items())
            msgs.insert(1, {"role": "system", "content": f"[会话事实] {facts_text}"})
        return msgs

    def compact(self, session_id: str) -> int:
        """手动触发压缩（自动裁剪在 ShortTermMemory 内已完成）；
        返回当前估算 Token 占用。生产实现此处应做"摘要替代历史"。"""
        ctx = self.get(session_id)
        ctx.memory._trim()
        from memory import _approx_tokens
        return sum(_approx_tokens(m.content) for m in ctx.memory.messages)

    def pause(self, session_id: str) -> None:
        self.get(session_id).status = "paused"

    def resume(self, session_id: str) -> None:
        self.get(session_id).status = "active"

    def close(self, session_id: str) -> None:
        """销毁会话：状态置 closed 并移出管理表（历史归档由调用方负责）。"""
        ctx = self.get(session_id)
        ctx.status = "closed"
        del self._sessions[session_id]

    @property
    def active_sessions(self) -> list[str]:
        return [s for s, c in self._sessions.items() if c.status == "active"]


if __name__ == "__main__":
    cm = ContextManager()
    cm.create("s1", "alice", "你是网络运维助手。", UserPreferences(role="engineer"))
    cm.append("s1", "user", "eNB-1024 告警严重吗")
    cm.add_fact("s1", "当前分析设备", "eNB-1024")
    msgs = cm.build_messages("s1")
    print(f"会话消息数: {len(msgs)}")
    print("注入的事实消息:", msgs[1]["content"])
    print("活跃会话:", cm.active_sessions)
    cm.pause("s1")
    print("暂停后活跃会话:", cm.active_sessions)
    cm.resume("s1")
    cm.close("s1")
    print("关闭后活跃会话:", cm.active_sessions)
