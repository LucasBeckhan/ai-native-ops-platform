#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/mcp/minimal/mcp_client.py
# 所属模块: 模块 29 —— 手写 MCP Client（stdio 子进程 / HTTP 双传输）
# 运行方式: 作为库被 Harness 的 mcp_manager 引用；单独运行自检
# 说明:
#   - Client 的职责（协议视角）：① 握手（initialize→initialized）；
#     ② 能力发现（tools/list 等）；③ 调用转发（tools/call）；
#     ④ 连接生命周期管理（进程拉起/销毁、健康检查）。
#   - stdio 传输：把 Server 作为子进程拉起，stdin/stdout 通信——
#     这是 MCP 最经典的形态（进程隔离，权限边界清晰）。
#   - HTTP 传输：POST /mcp 单请求单应答——适合容器化部署与远程 Server。
#   - 设计要点：调用序列化（同一连接同时只有一个在途请求，与官方
#     Client 语义一致）；读超时保护（Server 卡死不能拖垮 Harness）。
# =====================================================================
from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass

import httpx

from mcp_protocol import PROTOCOL_VERSION


@dataclass
class MCPServerInfo:
    name: str
    version: str
    capabilities: dict


class MCPClient:
    """单一连接的 MCP 客户端。stdio 与 http 二选一。"""

    def __init__(self, name: str = "teleops-client"):
        self.name = name
        self._next_id = 1
        self._lock = threading.Lock()          # 序列化在途请求
        self.server_info: MCPServerInfo | None = None

    # ---------------- 生命周期 ----------------
    def connect_stdio(self, command: list[str], timeout_s: float = 10.0) -> MCPServerInfo:
        """以子进程方式拉起 Server 并完成握手。"""
        self._proc = subprocess.Popen(
            command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, bufsize=1,
        )
        self._timeout_s = timeout_s
        self.server_info = self._initialize()
        return self.server_info

    def connect_http(self, base_url: str, timeout_s: float = 10.0) -> MCPServerInfo:
        """连接 HTTP 形态的 Server 并完成握手。"""
        self._base_url = base_url.rstrip("/") + "/mcp"
        self._http = httpx.Client(timeout=timeout_s, trust_env=False)  # 本机直连
        self.server_info = self._initialize()
        return self.server_info

    def close(self) -> None:
        if hasattr(self, "_proc") and self._proc.poll() is None:
            self._proc.terminate()             # 优雅终止 Server 子进程
            try:
                self._proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._proc.kill()

    # ---------------- 协议方法 ----------------
    def _initialize(self) -> MCPServerInfo:
        result = self._call("initialize", {
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": self.name, "version": "1.0.0"},
        })
        # 通知（无 id，无应答）——规范要求握手第三步
        self._notify("notifications/initialized")
        return MCPServerInfo(
            name=result["serverInfo"]["name"],
            version=result["serverInfo"]["version"],
            capabilities=result.get("capabilities", {}),
        )

    def list_tools(self) -> list[dict]:
        """能力发现：Server 支持哪些工具（动态发现是 MCP 的核心价值）。"""
        return self._call("tools/list", {})["tools"]

    def call_tool(self, name: str, arguments: dict) -> dict:
        """调用工具，返回 {content, isError}。isError=True 是业务失败，
        Client 不抛异常——由上层（Harness）决定如何处理。"""
        return self._call("tools/call", {"name": name, "arguments": arguments})

    def read_resource(self, uri: str) -> str:
        result = self._call("resources/read", {"uri": uri})
        return result["contents"][0]["text"]

    def get_prompt(self, name: str, arguments: dict) -> str:
        result = self._call("prompts/get", {"name": name, "arguments": arguments})
        return result["messages"][0]["content"]["text"]

    # ---------------- 传输实现 ----------------
    def _call(self, method: str, params: dict) -> dict:
        with self._lock:                        # 单在途请求（教学实现取简单正确）
            req_id = self._next_id
            self._next_id += 1
            wire = json.dumps({"jsonrpc": "2.0", "id": req_id,
                               "method": method, "params": params})
            if hasattr(self, "_proc"):
                raw = self._request_stdio(wire, req_id)
            else:
                raw = self._request_http(wire, req_id)
            resp = json.loads(raw)
            if "error" in resp:
                raise RuntimeError(f"MCP 错误 [{resp['error']['code']}]: {resp['error']['message']}")
            return resp["result"]

    def _notify(self, method: str) -> None:
        wire = json.dumps({"jsonrpc": "2.0", "method": method})
        if hasattr(self, "_proc"):
            self._proc.stdin.write(wire + "\n")
            self._proc.stdin.flush()
        else:
            self._request_http(wire, expect_response=False)

    def _request_stdio(self, wire: str, req_id: int) -> str:
        self._proc.stdin.write(wire + "\n")
        self._proc.stdin.flush()
        deadline = time.time() + self._timeout_s
        while time.time() < deadline:
            line = self._proc.stdout.readline()
            if not line:
                raise RuntimeError("MCP Server 子进程意外退出")
            resp = json.loads(line)
            if resp.get("id") == req_id:
                return line
            # 忽略不匹配的通知/响应（教学实现单在途，正常不会发生）
        raise TimeoutError(f"MCP 响应超时（>{self._timeout_s}s）: id={req_id}")

    def _request_http(self, wire: str, req_id=None, expect_response: bool = True) -> str:
        r = self._http.post(self._base_url, content=wire,
                            headers={"Content-Type": "application/json"})
        r.raise_for_status()
        if not expect_response:
            return ""
        return r.text


if __name__ == "__main__":
    # 自检：两种传输都跑一遍握手+发现+调用
    print("== stdio 传输 ==")
    c1 = MCPClient("selftest-stdio")
    info = c1.connect_stdio([sys.executable, "mcp_server.py", "--transport", "stdio"])
    print("握手成功:", info.name, info.version, "| 能力:", list(info.capabilities))
    print("工具发现:", [t["name"] for t in c1.list_tools()])
    r = c1.call_tool("query_kpis", {"slice_id": "slice-uRLLC-001"})
    print("调用结果:", r["content"][0]["text"][:80], "isError:", r["isError"])
    print("资源读取:", c1.read_resource("teleops://knowledge/alarm-playbook")[:50])
    print("提示模板:", c1.get_prompt("alarm-analysis", {"device_id": "eNB-1024"})[:50])
    c1.close()

    print("\n== http 传输 ==")
    import subprocess as sp
    srv = sp.Popen([sys.executable, "mcp_server.py", "--transport", "http", "--port", "18300"],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.0)
    try:
        c2 = MCPClient("selftest-http")
        info2 = c2.connect_http("http://127.0.0.1:18300")
        print("握手成功:", info2.name, "| 工具数:", len(c2.list_tools()))
        r2 = c2.call_tool("query_alarms", {"severity": "critical"})
        print("调用结果:", r2["content"][0]["text"][:70])
        c2.close()
    finally:
        srv.terminate()
