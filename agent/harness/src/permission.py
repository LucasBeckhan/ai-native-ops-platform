#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/harness/src/permission.py
# 所属模块: 模块 29 —— Harness 组件二：权限与安全沙箱（Permission & Sandbox）
# 运行方式: 作为库被 tool_router.py / agent_runtime.py 引用；可独立自检
# 说明:
#   - Harness 六大职责之一，回答"如何保证 Agent 不执行危险操作"。
#   - 权限模型（三维矩阵的简化教学版）：
#       用户角色 × 工具权限级别 → 裁决（allow / deny / require_approval）
#     工具在注册时声明 permission（read/write/dangerous），
#     角色能力表决定各角色可直达的级别。
#   - require_approval 的生产语义：工具调用被挂起，进入人工审批队列；
#     教学版记录审批事件并默认拒绝——演示"绝不静默放行"的底线。
#   - 所有裁决与审批都进入审计日志（Harness 安全职责的另一半，
#     模块 33 的行为审计直接消费这里的输出）。
# =====================================================================
from dataclasses import dataclass, field
from datetime import datetime, timezone

# 角色能力表：每个角色"无需审批"可直达的最高工具级别
ROLE_CAPABILITY = {
    "admin":    {"read": "allow", "write": "allow", "dangerous": "require_approval"},
    "engineer": {"read": "allow", "write": "require_approval", "dangerous": "deny"},
    "viewer":   {"read": "allow", "write": "deny", "dangerous": "deny"},
}


@dataclass
class AuditEvent:
    ts: str
    session_id: str
    user_id: str
    tool: str
    permission: str
    decision: str
    detail: str


@dataclass
class PermissionManager:
    session_id: str = ""
    user_id: str = ""
    role: str = "viewer"
    # 运行时审批放行名单：人工批准后把 (session, tool) 加入，本次会话内放行
    approved: set = field(default_factory=set)
    audit_log: list = field(default_factory=list)

    def check(self, tool: str, permission: str) -> tuple[str, str]:
        """核心裁决。返回 (decision, detail)。
        decision ∈ allow / deny / require_approval"""
        table = ROLE_CAPABILITY.get(self.role, ROLE_CAPABILITY["viewer"])
        decision = table.get(permission, "deny")
        detail = f"role={self.role} tool={tool} level={permission}"
        # 二次确认（审批）机制：已批准的写操作在本会话内放行
        if decision == "require_approval" and (self.session_id, tool) in self.approved:
            decision = "allow"
            detail += " [已人工批准]"
        self._audit(tool, permission, decision, detail)
        return decision, detail

    def approve(self, tool: str) -> None:
        """人工批准：只对 require_approval 有意义（deny 不可被批准——硬边界）。"""
        level = self._level_of(tool)
        if level == "dangerous":
            raise PermissionError(f"dangerous 级工具 {tool} 不可审批放行，必须由平台管理员改造")
        self.approved.add((self.session_id, tool))
        self._audit(tool, level, "approved_by_human", "人工批准，本会话内放行")

    def _level_of(self, tool: str) -> str:
        """由工具名反查级别（演示实现：按名字约定，生产应查工具注册表）。"""
        if any(k in tool for k in ("block", "reboot", "reset", "delete", "drop")):
            return "dangerous"
        if any(k in tool for k in ("set_", "add_", "update_", "execute_", "deploy")):
            return "write"
        return "read"

    def _audit(self, tool: str, permission: str, decision: str, detail: str) -> None:
        self.audit_log.append(AuditEvent(
            ts=datetime.now(timezone.utc).isoformat(),
            session_id=self.session_id, user_id=self.user_id,
            tool=tool, permission=permission, decision=decision, detail=detail,
        ))


if __name__ == "__main__":
    # 自检：三种角色 × 三种级别 = 裁决矩阵实测
    for role, tool, level in [
        ("admin", "query_alarms", "read"),        # → allow
        ("admin", "set_firewall_rule", "write"),  # → allow
        ("admin", "block_ip", "dangerous"),       # → require_approval
        ("engineer", "set_firewall_rule", "write"),  # → require_approval
        ("engineer", "block_ip", "dangerous"),    # → deny
        ("viewer", "set_firewall_rule", "write"), # → deny
    ]:
        pm = PermissionManager(session_id="s-test", user_id="u-1", role=role)
        decision, detail = pm.check(tool, level)
        print(f"{role:9} × {tool:20}({level:9}) → {decision}")

    # 审批流实测：engineer 的 write 操作先 require_approval，批准后放行
    pm = PermissionManager(session_id="s-2", user_id="u-2", role="engineer")
    d1, _ = pm.check("set_power_threshold", "write")
    print(f"\n审批前: {d1}")
    pm.approve("set_power_threshold")
    d2, detail = pm.check("set_power_threshold", "write")
    print(f"审批后: {d2} ({detail})")
    print(f"审计记录数: {len(pm.audit_log)}")
