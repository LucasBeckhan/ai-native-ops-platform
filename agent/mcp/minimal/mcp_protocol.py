#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/mcp/minimal/mcp_protocol.py
# 所属模块: 模块 29 —— MCP 协议核心数据结构（JSON-RPC 2.0）
# 运行方式: 作为库被 mcp_server.py / mcp_client.py 引用
# 说明:
#   - 为什么手写而不是用官方 SDK：MCP 的协议面很小（十来个方法），
#     手写一遍才能回答面试深水区问题（"initialize 握手交换了什么？
#     tools/list 和 resources/list 为什么分开？"）。官方 SDK 用法在
#     docs 里给出对照，生产建议用 SDK，学习必须懂协议。
#   - MCP 规范要点（2024-11-05 版本，本文件对齐的字段子集）：
#     * 传输层：stdio（换行分隔 JSON-RPC）或 Streamable HTTP（POST /mcp）
#     * 握手：client → initialize（协议版本+能力声明）→ server 应答
#       （能力+serverInfo）→ client 发 notifications/initialized → 开始工作
#     * 三类能力：tools（可调用函数）/ resources（可读数据）/
#       prompts（预置提示模板）——声明在 initialize 应答的 capabilities 里
# =====================================================================
import json
from dataclasses import dataclass, field

PROTOCOL_VERSION = "2024-11-05"


class MCPError(Exception):
    """协议层异常，可序列化为 JSON-RPC error 响应。"""

    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


# JSON-RPC 2.0 标准错误码 + MCP 扩展错误码
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
TOOL_NOT_FOUND = -32602        # MCP 约定复用 invalid params 语义


@dataclass
class JsonRpcRequest:
    jsonrpc: str
    id: int | str | None
    method: str
    params: dict = field(default_factory=dict)

    def is_notification(self) -> bool:
        return self.id is None


@dataclass
class JsonRpcResponse:
    jsonrpc: str
    id: int | str | None
    result: dict | None = None
    error: dict | None = None

    def to_dict(self) -> dict:
        out = {"jsonrpc": self.jsonrpc, "id": self.id}
        if self.error is not None:
            out["error"] = self.error
        else:
            out["result"] = self.result
        return out

    def to_wire(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


def parse_message(line: str) -> JsonRpcRequest:
    """把一行文本解析为 JSON-RPC 请求，失败抛 PARSE_ERROR。"""
    try:
        raw = json.loads(line)
    except json.JSONDecodeError as e:
        raise MCPError(PARSE_ERROR, f"JSON 解析失败: {e}")
    if raw.get("jsonrpc") != "2.0" or "method" not in raw:
        raise MCPError(INVALID_REQUEST, "缺少 jsonrpc:2.0 或 method 字段")
    return JsonRpcRequest(
        jsonrpc="2.0", id=raw.get("id"), method=raw["method"],
        params=raw.get("params") or {},
    )


def ok_response(req_id, result: dict) -> JsonRpcResponse:
    return JsonRpcResponse("2.0", req_id, result=result)


def err_response(req_id, code: int, message: str) -> JsonRpcResponse:
    return JsonRpcResponse("2.0", req_id, error={"code": code, "message": message})
