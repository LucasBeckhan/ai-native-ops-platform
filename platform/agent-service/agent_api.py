#!/usr/bin/env python3
# =====================================================================
# 文件名: platform/agent-service/agent_api.py
# 所属模块: 平台整合层 —— Agent 服务的 HTTP 封装（Harness 的网络入口）
# 运行方式:
#   本地: uvicorn agent_api:app --port 8100
#   容器: docker compose -f docker/docker-compose.agent.yml up -d
# 说明:
#   - 把模块 29 的 AgentRuntime 暴露为 HTTP API（对话 + 会话管理 + 审计），
#     这是"Agent 服务以 K8s/容器方式部署"（模块 34）的前置形态。
#   - 接口：POST /chat（对话）、POST /sessions（建会话）、
#     GET /sessions/{id}/report（会话报告）、GET /healthz、/metrics。
#   - MCP Server 以 HTTP 传输由 compose 独立容器运行，本服务启动时连接。
# =====================================================================
import os
import sys

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from prometheus_client import Counter, generate_latest
from pydantic import BaseModel

# 导入路径由 compose 的 PYTHONPATH 注入（/app/agent/harness/src 等）；
# 本地直跑时把仓库的 agent/ 目录加入路径即可
_here = os.path.dirname(os.path.abspath(__file__))
for p in (os.path.join(_here, "../../agent/harness/src"),
          os.path.join(_here, "../../agent/basics/m27-llm-adapter"),
          os.path.join(_here, "../../agent/basics/m28-agent-core"),
          os.path.join(_here, "../../agent/skills")):
    if os.path.isdir(p) and p not in sys.path:
        sys.path.insert(0, p)

from agent_runtime import AgentRuntime  # noqa: E402

app = FastAPI(title="TeleOps Agent Service", version="1.0.0")

CHAT_TOTAL = Counter("agent_chat_total", "对话请求数", ["status"])
TOOL_TOTAL = Counter("agent_tool_calls_total", "工具调用数（来自 Router 审计）")

# 运行时单例：MCP Server 地址由环境变量注入（容器网络 = mcp-server:8300）
MCP_URL = os.environ.get("MCP_HTTP_URL")            # 如 http://mcp-server:8300
SKILLS_DIR = os.environ.get("SKILLS_DIR", "/app/skills")
LLM_BASE_URL = os.environ.get("LLM_BASE_URL", "http://mock-llm:8000/v1")

_runtime: AgentRuntime | None = None


def get_runtime() -> AgentRuntime:
    global _runtime
    if _runtime is None:
        import llm_adapter
        # 容器内 Mock LLM 是 8000；本机直跑实验是 18000——由 LLM_BASE_URL 决定
        llm = llm_adapter.LLMAdapter(providers={
            "mock": llm_adapter.ProviderConfig(
                llm_adapter.Provider.OPENAI, LLM_BASE_URL, "MOCK_API_KEY", "mock-gpt")
        })
        _runtime = AgentRuntime(
            llm=llm,
            use_mcp=bool(MCP_URL),
            mcp_http_url=MCP_URL,
            skills_dir=SKILLS_DIR if os.path.isdir(SKILLS_DIR) else None,
        )
    return _runtime


class ChatIn(BaseModel):
    session_id: str
    message: str
    user_id: str = "anonymous"
    role: str = "engineer"


class SessionIn(BaseModel):
    session_id: str
    user_id: str
    role: str = "engineer"


@app.post("/sessions")
def create_session(body: SessionIn):
    try:
        get_runtime().create_session(body.session_id, body.user_id, role=body.role)
        return {"ok": True, "session_id": body.session_id, "role": body.role}
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@app.post("/chat")
def chat(body: ChatIn):
    rt = get_runtime()
    if body.session_id not in rt.contexts._sessions:
        rt.create_session(body.session_id, body.user_id, role=body.role)
    try:
        answer = rt.chat(body.session_id, body.message)
        CHAT_TOTAL.labels(status="ok").inc()
        return {"session_id": body.session_id, "answer": answer}
    except Exception as e:                              # noqa: BLE001
        CHAT_TOTAL.labels(status="error").inc()
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/sessions/{session_id}/report")
def session_report(session_id: str):
    rt = get_runtime()
    rep = rt.reports.get(session_id)
    if not rep:
        raise HTTPException(status_code=404, detail="会话不存在")
    return {
        "session_id": rep.session_id, "turns": rep.turns,
        "tool_calls": rep.tool_calls, "denied_calls": rep.denied_calls,
        "cost_usd": rep.cost_usd,
        "audit": [a["tool"] for a in rt.router.audit],
    }


@app.get("/healthz")
def healthz():
    get_runtime()   # 首次调用完成初始化（MCP 连接失败会在此暴露）
    return {"status": "ok", "mcp": bool(MCP_URL), "skills_dir": SKILLS_DIR}


@app.get("/metrics")
def metrics():
    return Response(content=generate_latest(), media_type="text/plain; charset=utf-8")
