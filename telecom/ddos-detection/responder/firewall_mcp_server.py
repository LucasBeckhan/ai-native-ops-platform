#!/usr/bin/env python3
# =====================================================================
# 文件名: telecom/ddos-detection/responder/firewall_mcp_server.py
# 所属模块: 场景二 —— 防火墙控制器 MCP Server（Agent 自动防御的执行面）
# 运行方式:
#   stdio: python3 firewall_mcp_server.py --transport stdio
#   HTTP : python3 firewall_mcp_server.py --transport http --port 8310
# 说明:
#   - 把"防火墙管理能力"以 MCP Server 形式暴露——场景二的核心设计：
#     Agent 通过 MCP 协议下发防御规则（不是直接调函数），权限、审计、
#     跨语言复用全部由协议层保证（模块 29 的理念落地到安全场景）。
#   - 三个工具的权限分级（对接 Harness 权限矩阵）：
#       list_blocked_ips        read       安全
#       add_firewall_rule      write      需审批（engineer 角色）
#       emergency_blackhole     dangerous  不可审批（admin 专用/人肉）
#   - 状态保存在本地 JSON 文件（/tmp/firewall_rules.json），重启不丢；
#     生产中该 Server 后面是真实的防火墙 API（如华为/飞塔管理面）。
# =====================================================================
import argparse
import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../agent/mcp/minimal"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../agent/basics/m28-agent-core"))

from mcp_protocol import (  # noqa: E402
    PROTOCOL_VERSION, MCPError, METHOD_NOT_FOUND, INTERNAL_ERROR,
    ok_response, err_response,
)
from tools import query_alarms  # noqa: E402  复用告警查询（跨 Server 组合演示）

RULES_FILE = "/tmp/firewall_rules.json"
_LOCK = threading.Lock()


def _load_rules() -> list[dict]:
    if os.path.exists(RULES_FILE):
        with open(RULES_FILE, encoding="utf-8") as f:
            return json.load(f)
    return []


def _save_rules(rules: list[dict]) -> None:
    with open(RULES_FILE, "w", encoding="utf-8") as f:
        json.dump(rules, f, ensure_ascii=False, indent=1)


# ---------------------------------------------------------------------------
# 工具实现（真实语义：下发规则 → 确认生效 → 记录审计时间线）
# ---------------------------------------------------------------------------
def list_blocked_ips(limit: int = 50) -> dict:
    with _LOCK:
        rules = _load_rules()
    return {"total": len(rules), "rules": rules[:limit]}


def add_firewall_rule(ip: str, reason: str = "ddos-mitigation",
                       duration_minutes: int = 30) -> dict:
    """下发封禁规则。含三个生产要素：
    ① IP 格式校验（LLM 参数不可信）；② 幂等（重复封禁不重复添加）；
    ③ 自动过期时间（防"封了忘解"的事故）。"""
    import re
    if not re.match(r"^(\d{1,3}\.){3}\d{1,3}$", ip):
        return {"ok": False, "error": f"非法 IP 格式: {ip}"}
    with _LOCK:
        rules = _load_rules()
        for r in rules:
            if r["ip"] == ip and r["status"] == "active":
                return {"ok": True, "already_blocked": True, "rule": r}
        rule = {
            "ip": ip, "reason": reason, "status": "active",
            "created_at": time.time(),
            "expires_at": time.time() + duration_minutes * 60,
            "action": "drop",   # 模拟防火墙动作：丢弃该源流量
        }
        rules.append(rule)
        _save_rules(rules)
    return {"ok": True, "already_blocked": False, "rule": rule,
            "confirm": f"防火墙已下发 drop 规则: {ip}（{duration_minutes} 分钟后自动解除）"}


def emergency_blackhole(target_ip: str) -> dict:
    """黑洞路由（dangerous 级）：整机牵引，影响业务，必须人工决策。"""
    return {"ok": False,
            "error": "emergency_blackhole 属于 dangerous 级操作：Harness 权限矩阵会拒绝/挂起。"
                     "生产中该操作绑定运维工单 + 双人复核，不开放给 Agent 自动执行。"}


TOOLS_DEF = [
    {"name": "list_blocked_ips",
     "description": "查询当前防火墙封禁列表（IP/原因/到期时间）。",
     "inputSchema": {"type": "object",
                     "properties": {"limit": {"type": "integer"}}}},
    {"name": "add_firewall_rule",
     "description": "下发封禁规则：丢弃指定源 IP 的流量。用于 DDoS 防御处置。"
                    "ip 必须是合法 IPv4；duration_minutes 默认 30 分钟自动解除。",
     "inputSchema": {"type": "object",
                     "properties": {
                         "ip": {"type": "string", "description": "攻击源 IPv4"},
                         "reason": {"type": "string"},
                         "duration_minutes": {"type": "integer"},
                     },
                     "required": ["ip"]}},
    {"name": "emergency_blackhole",
     "description": "黑洞路由（危险操作，仅演示权限拦截）。",
     "inputSchema": {"type": "object",
                     "properties": {"target_ip": {"type": "string"}},
                     "required": ["target_ip"]}},
]

TOOL_IMPL = {
    "list_blocked_ips": list_blocked_ips,
    "add_firewall_rule": add_firewall_rule,
    "emergency_blackhole": emergency_blackhole,
}


class FirewallMCPServerCore:
    """与 teleops-mcp-server 相同的协议分发骨架（复制而非继承——
    教学取向让每个 Server 自包含可读；生产应抽公共库）。"""

    def handle(self, req) -> dict:
        m = req.method
        if m == "initialize":
            return {"protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "firewall-mcp-server", "version": "1.0.0"}}
        if m == "notifications/initialized":
            return {}
        if m == "ping":
            return {}
        if m == "tools/list":
            return {"tools": TOOLS_DEF}
        if m == "tools/call":
            return self._tools_call(req.params)
        raise MCPError(METHOD_NOT_FOUND, f"未知方法: {m}")

    @staticmethod
    def _tools_call(params: dict) -> dict:
        name = params.get("name", "")
        args = params.get("arguments", {})
        if name not in TOOL_IMPL:
            raise MCPError(-32602, f"工具不存在: {name}")
        try:
            result = TOOL_IMPL[name](**args)
        except TypeError as e:
            return {"content": [{"type": "text", "text": f"参数错误: {e}"}], "isError": True}
        except Exception as e:                    # noqa: BLE001
            return {"content": [{"type": "text", "text": f"执行失败: {e}"}], "isError": True}
        return {"content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}],
                "isError": False}


def serve_stdio() -> None:
    core = FirewallMCPServerCore()
    from mcp_protocol import parse_message
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        req_raw = json.loads(line)
        try:
            parsed = parse_message(line)
            result = core.handle(parsed)
            resp = parsed.is_notification() and None or ok_response(parsed.id, result)
        except MCPError as e:
            resp = err_response(req_raw.get("id"), e.code, e.message)
        except Exception as e:                    # noqa: BLE001
            resp = err_response(req_raw.get("id"), INTERNAL_ERROR, str(e))
        if resp is not None:
            sys.stdout.write(resp.to_wire() + "\n")
            sys.stdout.flush()


def serve_http(port: int) -> None:
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    core = FirewallMCPServerCore()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):                        # noqa: N802
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode()
            try:
                from mcp_protocol import parse_message
                parsed = parse_message(body)
                result = core.handle(parsed)
                resp = ok_response(parsed.id, result)
            except MCPError as e:
                resp = err_response(None, e.code, e.message)
            except Exception as e:                # noqa: BLE001
                resp = err_response(None, INTERNAL_ERROR, str(e))
            payload = resp.to_wire().encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *a):
            pass

    print(f"Firewall MCP Server (HTTP) :{port}/mcp", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="防火墙控制器 MCP Server")
    parser.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    parser.add_argument("--port", type=int, default=8310)
    args = parser.parse_args()
    (serve_http(args.port) if args.transport == "http" else serve_stdio())
