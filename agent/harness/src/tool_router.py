#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/harness/src/tool_router.py
# 所属模块: 模块 29 —— Harness 组件三：工具调度与路由（Tool Orchestration）
# 运行方式: 作为库被 agent_runtime.py 引用；可独立自检
# 说明:
#   - Harness 六大职责之一：接收 Agent 的工具调用请求 → 权限校验 →
#     路由到执行器（本地注册表 或 MCP Server）→ 结果归一化返回。
#   - 核心抽象：**Agent 只面向统一的工具 Schema 与 execute() 接口**，
#     工具来自进程内函数还是远程 MCP Server 完全透明——这正是
#     "对比实验"（直接函数调用 vs MCP）能只改一行配置的原因。
#   - MCP 工具的 Schema 转换：MCP 的 inputSchema 与 OpenAI 的 parameters
#     结构相同（都是 JSON Schema），仅需字段名映射。
#   - 路由器同时聚合两个来源的工具清单（本地 + 各 MCP Server）——
#     多 Server 并存是 MCP 的设计目标之一。
# =====================================================================
from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../basics/m28-agent-core"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../mcp/minimal"))

from permission import PermissionManager  # noqa: E402
from tools import ToolRegistry, build_default_registry  # noqa: E402


@dataclass
class MCPServerHandle:
    """一个已连接 MCP Server 的句柄（工具名前缀防冲突：server.tool）。"""
    alias: str
    client: object                       # mcp_client.MCPClient
    tools: dict = field(default_factory=dict)   # name → schema


class ToolRouter:
    """统一工具入口：本地注册表 + N 个 MCP Server 的聚合路由。"""

    def __init__(self, local_registry: ToolRegistry | None = None,
                 use_mcp: bool = False, mcp_command: list[str] | None = None,
                 mcp_http_url: str | None = None):
        self.local = local_registry or build_default_registry()
        self.mcp_servers: dict[str, MCPServerHandle] = {}
        self.audit: list[dict] = []
        if use_mcp:
            self._connect_mcp(mcp_command, mcp_http_url)

    # ---------------- MCP 接入 ----------------
    def _connect_mcp(self, command: list[str] | None, http_url: str | None) -> None:
        from mcp_client import MCPClient
        client = MCPClient("harness-router")
        if command:
            info = client.connect_stdio(command)
            alias = info.name
        elif http_url:
            info = client.connect_http(http_url)
            alias = info.name + "-http"
        else:
            raise ValueError("use_mcp=True 需要 mcp_command 或 mcp_http_url")
        # 动态发现：Server 提供哪些工具由 Server 决定（MCP 核心价值）
        tools = {t["name"]: t for t in client.list_tools()}
        self.mcp_servers[alias] = MCPServerHandle(alias=alias, client=client, tools=tools)

    def close(self) -> None:
        for h in self.mcp_servers.values():
            h.client.close()

    # ---------------- 工具清单聚合 ----------------
    def openai_schemas(self) -> list[dict]:
        """聚合本地与 MCP 工具，输出统一的 OpenAI tools 声明。
        MCP 工具名加前缀 `alias.name` 防止多 Server 之间的命名冲突。"""
        schemas = list(self.local.openai_schemas())
        for handle in self.mcp_servers.values():
            for name, t in handle.tools.items():
                schemas.append({
                    "type": "function",
                    "function": {
                        "name": f"{handle.alias}.{name}",
                        "description": t.get("description", ""),
                        # MCP inputSchema ≡ OpenAI parameters（同为 JSON Schema）
                        "parameters": t.get("inputSchema", {"type": "object", "properties": {}}),
                    },
                })
        return schemas

    # ---------------- 名字解析（Skill 层使用）----------------
    def resolve(self, base_name: str) -> str | None:
        """按工具"短名"解析实际可调用名：本地优先，其次 MCP 前缀名。
        Skill/Agent 代码只写短名，路由细节由 Harness 屏蔽。"""
        if base_name in self.local._tools:
            return base_name
        for handle in self.mcp_servers.values():
            if base_name in handle.tools:
                return f"{handle.alias}.{base_name}"
        return None

    # ---------------- 执行路由 ----------------
    def execute(self, name: str, args: dict, perm: PermissionManager,
                timeout_s: float = 15.0) -> str:
        """Agent 侧唯一入口。步骤：权限裁决 → 路由 → 执行 → 审计。"""
        # 1. 权限裁决（MCP 工具级别按名字反查约定，本地工具读注册表声明）
        if "." in name:
            level = self._mcp_level(name)
        else:
            level = self.local.get(name).permission
        decision, detail = perm.check(name, level)
        if decision == "deny":
            return f"[权限拒绝] {detail}（该操作已被 Harness 拦截）"
        if decision == "require_approval":
            return (f"[需人工审批] {detail}。请提交审批；"
                    f"批准后本会话内可执行（演示环境默认挂起）。")

        # 2. 路由：本地 or MCP
        t0 = time.time()
        if "." in name:
            alias, tool_name = name.split(".", 1)
            result = self._execute_mcp(alias, tool_name, args, timeout_s)
        else:
            try:
                result = json.dumps(self.local.get(name).func(**args), ensure_ascii=False)
            except Exception as e:                          # noqa: BLE001
                result = f"[工具执行异常] {type(e).__name__}: {e}"
        self.audit.append({
            "tool": name, "args": args, "ms": int((time.time() - t0) * 1000),
            "decision": "executed", "detail": detail,
        })
        return result

    @staticmethod
    def _execute_mcp(alias: str, tool_name: str, args: dict, timeout_s: float) -> str:
        """MCP 工具调用：超时保护 + isError 语义透传。"""
        import concurrent.futures
        handle = TOOL_ROUTER_CONTEXT.mcp_servers[alias]     # 见文件尾说明
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(handle.client.call_tool, tool_name, args)
            try:
                resp = future.result(timeout=timeout_s)
            except concurrent.futures.TimeoutError:
                return f"[MCP 超时] {alias}.{tool_name} 在 {timeout_s}s 内未响应"
        text = resp["content"][0]["text"] if resp.get("content") else "(空结果)"
        if resp.get("isError"):
            return f"[MCP 业务失败] {text}"
        return text

    @staticmethod
    def _mcp_level(name: str) -> str:
        """MCP 工具的权限级别推断（演示约定，生产应由 Server 声明 + Host 复核）。"""
        short = name.split(".", 1)[1]
        if any(k in short for k in ("block", "reboot", "reset", "delete", "drop")):
            return "dangerous"
        if any(k in short for k in ("set_", "add_", "update_", "execute_")):
            return "write"
        return "read"


class _RouterContextHolder:
    """ThreadPoolExecutor 内无法传 self 的临时桥接（教学实现的取舍）；
    生产实现应把 handle 直接闭包进 _execute_mcp。"""
    mcp_servers: dict = {}


TOOL_ROUTER_CONTEXT = _RouterContextHolder()


def new_router(**kwargs) -> ToolRouter:
    """工厂：创建路由器并完成上下文桥接。"""
    router = ToolRouter(**kwargs)
    TOOL_ROUTER_CONTEXT.mcp_servers = router.mcp_servers
    return router


if __name__ == "__main__":
    # 自检一：纯本地模式
    router = new_router()
    perm = PermissionManager(session_id="s-selftest", user_id="u-1", role="admin")
    print("本地模式工具数:", len(router.openai_schemas()))
    print("本地调用:", router.execute("query_alarms", {"device_id": "eNB-1024"}, perm)[:60], "...")
    # 权限拦截演示：临时注册一个 write 工具，viewer 角色调用应被拒绝
    def set_power_threshold(device_id: str, threshold: float) -> dict:
        return {"device_id": device_id, "threshold": threshold, "applied": True}
    from tools import Tool
    router.local.register(Tool(
        name="set_power_threshold", description="修改光功率阈值（写操作演示）",
        parameters={"type": "object",
                    "properties": {"device_id": {"type": "string"},
                                   "threshold": {"type": "number"}},
                    "required": ["device_id"]},
        func=set_power_threshold, permission="write",
    ))
    perm_viewer = PermissionManager(session_id="s-selftest", user_id="u-1", role="viewer")
    print("权限拦截(viewer×write):", router.execute(
        "set_power_threshold", {"device_id": "eNB-1024", "threshold": -24.0}, perm_viewer))
    perm_eng = PermissionManager(session_id="s-selftest", user_id="u-1", role="engineer")
    print("审批挂起(engineer×write):", router.execute(
        "set_power_threshold", {"device_id": "eNB-1024", "threshold": -24.0}, perm_eng)[:40], "...")
