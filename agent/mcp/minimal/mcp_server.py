#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/mcp/minimal/mcp_server.py
# 所属模块: 模块 29 —— 手写 MCP Server（tools/resources/prompts 三能力）
# 运行方式:
#   stdio 模式（被 MCP Client 子进程拉起）: python3 mcp_server.py --transport stdio
#   HTTP  模式（容器/跨机访问）:            python3 mcp_server.py --transport http --port 8300
# 说明:
#   - Server 的职责：声明能力（initialize 应答）+ 分发请求（tools/call 等）
#     + 把工具实现的结果包装为 MCP content 格式（text/结构化）。
#   - 工具实现复用模块 28 的 tools.py——同一份工具能力，
#     模块 28 以进程内函数接入，本模块以协议方式暴露，对比实验的基础。
#   - resources：暴露告警知识文件（resources/read）；prompts：暴露
#     "告警分析"提示模板（prompts/get）——三类能力的差异在教学上一目了然。
# =====================================================================
import argparse
import json
import sys
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../basics/m28-agent-core"))

from mcp_protocol import (  # noqa: E402
    PROTOCOL_VERSION, MCPError, PARSE_ERROR, METHOD_NOT_FOUND,
    INTERNAL_ERROR, JsonRpcRequest, ok_response, err_response,
)
from tools import build_default_registry, query_alarms, query_kpis, run_diagnosis  # noqa: E402

# ---------------------------------------------------------------------------
# 能力一：Tools —— 可调用的函数（Agent 主动调用）
# ---------------------------------------------------------------------------
TOOLS_DEF = [
    {
        "name": "query_alarms",
        "description": "查询网络设备告警。device_id 传 eNB-xxxx；severity 可选 critical/major/minor。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "device_id": {"type": "string", "description": "设备编号"},
                "severity": {"type": "string", "enum": ["critical", "major", "minor"]},
                "limit": {"type": "integer"},
            },
        },
    },
    {
        "name": "query_kpis",
        "description": "查询 5G 切片 KPI（吞吐/时延/丢包/在线用户数）。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "slice_id": {"type": "string", "description": "切片ID"},
                "window": {"type": "string", "description": "时间窗"},
            },
            "required": ["slice_id"],
        },
    },
    {
        "name": "run_diagnosis",
        "description": "对设备运行诊断脚本（CPU/内存/光功率/链路/配置差异）。",
        "inputSchema": {
            "type": "object",
            "properties": {"device_id": {"type": "string"}},
            "required": ["device_id"],
        },
    },
    {
        "name": "es_query_alerts",
        "description": "查询平台告警索引（telecom-alerts）最近的告警。index 支持按前缀过滤，size 上限 50。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "index": {"type": "string", "description": "索引名或前缀，默认 telecom-alerts"},
                "size": {"type": "integer", "description": "返回条数，默认 10"},
                "keyword": {"type": "string", "description": "可选关键字过滤（match 查询字段 message/type）"},
            },
        },
    },
]

def _es_query_alerts(index: str = "telecom-alerts", size: int = 10,
                     keyword: str = "") -> dict:
    """查询 Elasticsearch 告警索引（场景一的 SLA 分析数据面）。
    ES 地址在容器网络内是 elasticsearch:9200（prometheus.yml 同源约定）；
    本机直跑时回退 localhost:9200。"""
    import json as _json
    import urllib.request
    hosts = ["http://elasticsearch:9200", "http://localhost:9200"]
    size = max(1, min(int(size), 50))
    body = {"query": {"match_all": {}}, "sort": [{"@timestamp": {"order": "desc"}}],
            "size": size}
    if keyword:
        body["query"] = {"bool": {"should": [
            {"match": {"message": keyword}},
            {"match": {"type": keyword}},
            {"match": {"slice_id": keyword}},
            {"match": {"device_id": keyword}},
        ]}}
    last_err = None
    for host in hosts:
        try:
            req = urllib.request.Request(
                f"{host}/{index}/_search",
                data=_json.dumps(body).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=5) as r:
                data = _json.loads(r.read())
            hits = [_hit_source(h) for h in data["hits"]["hits"]]
            return {"total": data["hits"]["total"]["value"], "items": hits}
        except Exception as e:                    # noqa: BLE001 —— 多地址依次回退
            last_err = e
    raise RuntimeError(f"ES 查询失败（所有地址）: {last_err}")


def _hit_source(hit: dict) -> dict:
    src = hit.get("_source", {})
    # 统一字段名视图，屏蔽 @timestamp 与 ts 的差异
    src["ts"] = src.get("@timestamp") or src.get("ts")
    return src


TOOL_IMPL = {
    "query_alarms": query_alarms,
    "query_kpis": query_kpis,
    "run_diagnosis": run_diagnosis,
    "es_query_alerts": _es_query_alerts,
}


# ---------------------------------------------------------------------------
# 能力二：Resources —— 可读取的数据源（Agent 拉取型知识）
# ---------------------------------------------------------------------------
RESOURCES = {
    "teleops://knowledge/alarm-playbook": {
        "uri": "teleops://knowledge/alarm-playbook",
        "name": "告警处置手册",
        "mimeType": "text/plain",
        "text": "光模块接收异常处置流程：1) 确认收光功率是否低于阈值-24dBm；"
                "2) 检查尾纤弯折与接口清洁；3) 必要时更换光模块并通知传输组核查对端。",
    },
    "teleops://knowledge/sla-thresholds": {
        "uri": "teleops://knowledge/sla-thresholds",
        "name": "切片SLA阈值表",
        "mimeType": "text/plain",
        "text": "eMBB: 时延<=20ms 丢包<=0.5% | uRLLC: 时延<=10ms 丢包<=0.1%",
    },
}

# ---------------------------------------------------------------------------
# 能力三：Prompts —— 预置提示模板（宿主按需取用）
# ---------------------------------------------------------------------------
PROMPTS = {
    "alarm-analysis": {
        "name": "alarm-analysis",
        "description": "告警分析模板：输入设备ID，输出结构化分析提示词",
        "arguments": [{"name": "device_id", "description": "设备编号", "required": True}],
        # prompts/get 返回可直接拼入对话的 messages
        "template": "请分析设备 {device_id} 的当前告警：先查询告警列表，"
                    "再对最高级别告警运行诊断，最后按严重程度排序给出处置建议。",
    },
}


class MCPServerCore:
    """协议核心：与传输层解耦（stdio/HTTP 共用同一套分发逻辑）。"""

    def __init__(self) -> None:
        self.initialized_sessions: set[str] = set()

    def handle(self, req: JsonRpcRequest) -> dict:
        """分发一个已解析的 JSON-RPC 请求，返回 result 字典。
        notifications（无 id）返回空 dict 由传输层决定是否忽略。"""
        m = req.method
        if m == "initialize":
            # 握手：交换协议版本、能力清单、服务信息——Client 由此得知
            # 本 Server 支持 tools/resources/prompts 哪些能力
            return {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {
                    "tools": {"listChanged": True},
                    "resources": {"subscribe": False},
                    "prompts": {"listChanged": False},
                },
                "serverInfo": {"name": "teleops-mcp-server", "version": "1.0.0"},
            }
        if m == "notifications/initialized":
            return {}                       # 通知无需应答
        if m == "ping":
            return {}
        if m == "tools/list":
            return {"tools": TOOLS_DEF}
        if m == "tools/call":
            return self._tools_call(req.params)
        if m == "resources/list":
            return {"resources": [{k: v[k] for k in ("uri", "name", "mimeType")}
                                   for v in RESOURCES.values()]}
        if m == "resources/read":
            uri = req.params.get("uri", "")
            if uri not in RESOURCES:
                raise MCPError(-32602, f"资源不存在: {uri}")
            r = RESOURCES[uri]
            return {"contents": [{"uri": uri, "mimeType": r["mimeType"], "text": r["text"]}]}
        if m == "prompts/list":
            return {"prompts": [{k: v[k] for k in ("name", "description", "arguments")}
                                 for v in PROMPTS.values()]}
        if m == "prompts/get":
            name = req.params.get("name", "")
            if name not in PROMPTS:
                raise MCPError(-32602, f"提示模板不存在: {name}")
            args = {a["name"]: req.params.get("arguments", {}).get(a["name"], "")
                    for a in PROMPTS[name]["arguments"]}
            text = PROMPTS[name]["template"].format(**args)
            return {"messages": [{"role": "user", "content": {"type": "text", "text": text}}]}
        raise MCPError(METHOD_NOT_FOUND, f"未知方法: {m}")

    @staticmethod
    def _tools_call(params: dict) -> dict:
        """tools/call 的统一包装：入参校验 → 执行 → MCP content 格式。
        isError=true 语义：工具执行了但业务失败（区别于协议层错误）。"""
        name = params.get("name", "")
        args = params.get("arguments", {})
        if name not in TOOL_IMPL:
            raise MCPError(-32602, f"工具不存在: {name}")
        try:
            result = TOOL_IMPL[name](**args)
        except TypeError as e:
            return {"content": [{"type": "text", "text": f"参数错误: {e}"}], "isError": True}
        except Exception as e:                  # noqa: BLE001 —— 业务失败走 isError
            return {"content": [{"type": "text", "text": f"执行失败: {e}"}], "isError": True}
        return {
            "content": [{"type": "text", "text": json.dumps(result, ensure_ascii=False)}],
            "isError": False,
        }


def serve_stdio() -> None:
    """stdio 传输：一行一个 JSON-RPC 消息（MCP 规范的 stdio 约定）。"""
    core = MCPServerCore()
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            from mcp_protocol import parse_message
            parsed = parse_message(line)
            result = core.handle(parsed)
            if parsed.is_notification():
                continue                        # 通知不应答
            resp = ok_response(parsed.id, result)
        except MCPError as e:
            resp = err_response(req.get("id"), e.code, e.message)
        except Exception as e:                  # noqa: BLE001
            resp = err_response(req.get("id"), INTERNAL_ERROR, str(e))
        sys.stdout.write(resp.to_wire() + "\n")
        sys.stdout.flush()                      # 必须逐行刷出，否则 Client 读不到


def serve_http(port: int) -> None:
    """Streamable HTTP（无状态简化版）：POST /mcp 单请求单应答。"""
    core = MCPServerCore()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):                       # noqa: N802
            if self.path != "/mcp":
                self.send_error(404)
                return
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode()
            try:
                parsed = json.loads(body)
                from mcp_protocol import parse_message
                req = parse_message(body)
                result = core.handle(req)
                resp = ok_response(req.id, result)
            except MCPError as e:
                resp = err_response(parsed.get("id") if isinstance(parsed, dict) else None,
                                    e.code, e.message)
            except Exception as e:              # noqa: BLE001
                resp = err_response(None, INTERNAL_ERROR, str(e))
            payload = resp.to_wire().encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *a):              # 静默访问日志，保持实验输出干净
            pass

    print(f"MCP Server (HTTP) 监听 :{port}/mcp —— 协议版本 {PROTOCOL_VERSION}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="手写 MCP Server（教学实现）")
    parser.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    parser.add_argument("--port", type=int, default=8300)
    args = parser.parse_args()
    if args.transport == "http":
        serve_http(args.port)
    else:
        serve_stdio()
