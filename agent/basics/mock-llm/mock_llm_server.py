# =====================================================================
# 文件名: agent/basics/mock-llm/mock_llm_server.py
# 所属模块: 基础设施层 —— Mock LLM Server（模块 26/27 会详细拆解其实现）
# 运行方式:
#   本地运行: pip install -r requirements.txt && uvicorn mock_llm_server:app --port 8000
#   容器运行: docker compose -f docker/docker-compose.llm.yml up -d
# 说明:
#   - 提供 OpenAI 兼容 API：/v1/chat/completions（含流式 SSE 和工具调用）、
#     /v1/embeddings、/healthz、/metrics。学习者的代码只要面向 OpenAI 协议
#     编写，把 BASE_URL 指向本服务即可离线跑通全部 Agent 实验，无需真实 API Key。
#   - “智能”来自确定性规则：识别告警类问题 → 返回 tool_calls；识别到工具
#     结果 → 生成总结回复。这让我们能稳定复现 Function Calling 的每一步，
#     也让单元测试可以断言（真实 LLM 输出不稳定，这是工程化的重要知识点）。
#   - 接口协议与真实厂商一致，因此切换真实模型只需改 BASE_URL + API_KEY。
# =====================================================================

import hashlib
import json
import math
import re
import time
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from prometheus_client import Counter, Gauge, generate_latest

app = FastAPI(title="Mock LLM Server", version="1.0.0")

# ---------- 可观测性指标（Prometheus 抓取 /metrics） ----------
REQUEST_TOTAL = Counter(
    "llm_requests_total", "收到的 LLM 请求数", ["model", "mode"]
)  # mode: chat / chat_stream / tool_call / embeddings
PROMPT_TOKENS = Counter("llm_prompt_tokens_total", "Prompt Token 消耗总量", ["model"])
COMPLETION_TOKENS = Counter("llm_completion_tokens_total", "补全 Token 消耗总量", ["model"])
INFLIGHT = Gauge("llm_inflight_requests", "正在处理的请求数")

# 工具结果 → 摘要话术的映射，让回复内容与实验场景贴合
_TOOL_RESULT_TEMPLATES = {
    "query_alarms": "共查询到 {n} 条相关告警，其中最高级别为 {sev}。",
    "query_kpis": "KPI 查询完成：{n} 条指标记录，已进行阈值比对。",
    "run_diagnosis": "诊断流程执行完成，共 {n} 个步骤返回结果。",
    "execute_firewall_block": "防火墙指令已下发并确认生效。",
}


def _approx_tokens(text: str) -> int:
    """粗略估算 Token 数：中文按字算、英文按 4 字符 1 Token 估算。
    真实 Tokenizer 用 BPE 切分，这里只要量级正确即可支撑成本核算实验。"""
    if not text:
        return 0
    cjk = sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")
    return cjk + max(1, (len(text) - cjk) // 4)


def _pick_tool(tools: list[dict], text: str) -> dict | None:
    """根据用户文本 + 工具声明的名称/描述做关键词匹配，选出要调用的工具。
    真实 LLM 是“阅读工具 Schema 后推理选择”，这里用规则模拟同样的决策面。"""
    rules = [
        ("告警", ["query_alarms", "list_alarms", "search_alarms"]),
        ("根因", ["run_diagnosis", "diagnose", "analyze_root_cause"]),
        ("诊断", ["run_diagnosis", "diagnose"]),
        ("流量", ["query_kpis", "query_traffic", "get_flow_stats"]),
        ("KPI", ["query_kpis"]),
        ("切片", ["query_kpis", "query_alarms"]),
        ("封禁", ["execute_firewall_block", "block_ip"]),
        ("防火墙", ["execute_firewall_block", "add_firewall_rule"]),
    ]
    tool_names = {t.get("function", {}).get("name", ""): t for t in tools}
    for keyword, candidates in rules:
        if keyword in text:
            for name in candidates:
                if name in tool_names:
                    return tool_names[name]
    # 关键词未命中时退化为：调用声明顺序中的第一个工具（保持行为可预期）
    return tools[0] if tools else None


def _build_arguments(tool: dict, text: str) -> str:
    """根据工具参数 Schema 的字段名做朴素槽位填充。
    例如字段名含 'id' 就尝试从文本中抓 eNB-xxxx/slice-xxxx/IP 形态的实体。"""
    params: dict[str, Any] = {}
    func = tool.get("function", {})
    schema = func.get("parameters", {}).get("properties", {})
    # 从文本中抽取常见实体
    ids = re.findall(r"[A-Za-z]+[-_]?\d{2,}", text)
    ips = re.findall(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", text)
    for field, spec in schema.items():
        ftype = spec.get("type", "string")
        if ftype == "array":
            continue      # 列表参数无法从自由文本可靠填充，交给工具的默认值
        low = field.lower()
        if "ip" in low and ips:
            params[field] = ips[0]
        elif ("id" in low or "name" in low) and ids:
            params[field] = ids[0]
        elif ftype == "integer":
            params[field] = 10
        elif ftype == "number":
            params[field] = 0.9
        elif "time" in low or "window" in low:
            params[field] = "last_1h"
        elif "severity" in low or "level" in low:
            params[field] = "critical" if any(w in text for w in ["紧急", "critical", "严重"]) else "major"
        else:
            params[field] = text[:64]
    if not params:
        params = {"query": text[:64]}
    return json.dumps(params, ensure_ascii=False)


def _tool_result_payload(message: dict) -> str:
    """把 OpenAI 协议中 role=tool 的消息内容解析为可读文本。
    兼容两种传入格式：字符串，或 {"content": "..."} 结构。"""
    content = message.get("content", "")
    if isinstance(content, str):
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return content
        return json.dumps(data, ensure_ascii=False)
    return json.dumps(content, ensure_ascii=False)


def _compose_final_answer(messages: list[dict]) -> str:
    """当对话中已存在工具执行结果时，生成面向运维工程师的最终答复。"""
    tool_msgs = [m for m in messages if m.get("role") == "tool"]
    user_q = next((m["content"] for m in reversed(messages) if m.get("role") == "user"), "")
    lines = [f"已完成分析，结论如下（问题：{user_q}）"]
    for i, tm in enumerate(tool_msgs, 1):
        name = tm.get("name") or tm.get("tool_call_id", f"tool_{i}")
        payload = _tool_result_payload(tm)
        # 尝试统计结果条数，让答复更贴近真实运维汇报口吻
        n = 0
        sev = "unknown"
        try:
            data = json.loads(payload)
            if isinstance(data, list):
                n = len(data)
                sev = str(data[0].get("severity", "unknown")) if data and isinstance(data[0], dict) else sev
            elif isinstance(data, dict) and "items" in data:
                n = len(data["items"])
        except (json.JSONDecodeError, TypeError):
            n = 1
        template = _TOOL_RESULT_TEMPLATES.get(name, "工具 {name} 执行成功。")
        lines.append(f"{i}. " + template.format(n=n, sev=sev, name=name))
        lines.append(f"   工具返回摘要: {payload[:160]}")
    lines.append("建议下一步：1) 优先处理最高级别告警；2) 结合历史工单判断是否为已知问题；3) 如需自动处置，请授权我执行对应 Skill。")
    return "\n".join(lines)


def _decide(messages: list[dict], tools: list[dict]) -> dict:
    """核心决策函数：根据对话上下文决定“调用工具”还是“直接回答”。"""
    has_tool_result = any(m.get("role") == "tool" for m in messages)
    last_user = next((m["content"] for m in reversed(messages) if m.get("role") == "user"), "")
    if tools and not has_tool_result and last_user:
        tool = _pick_tool(tools, str(last_user))
        if tool:
            func = tool["function"]
            return {
                "kind": "tool_call",
                "tool_call": {
                    "id": f"call_{int(time.time() * 1000)}",
                    "type": "function",
                    "function": {
                        "name": func["name"],
                        "arguments": _build_arguments(tool, str(last_user)),
                    },
                },
            }
    if has_tool_result:
        # 多步链支持：已查过告警且用户追问原因/诊断/建议 → 追加诊断工具调用；
        # 诊断也已执行（或无诊断工具）→ 收敛为最终答案。
        tool_msgs = [m for m in messages if m.get("role") == "tool"]
        diagnosed = any(
            (m.get("name") or "").startswith("run_diagnosis") or
            (m.get("name") or "").startswith("diagnose")
            for m in tool_msgs
        )
        wants_diagnosis = any(w in str(last_user) for w in ["原因", "诊断", "建议", "根因"])
        if not diagnosed and wants_diagnosis:
            diag = next((t for t in tools if t.get("function", {}).get("name")
                         in ("run_diagnosis", "diagnose")), None)
            if diag:
                func = diag["function"]
                return {
                    "kind": "tool_call",
                    "tool_call": {
                        "id": f"call_{int(time.time() * 1000)}",
                        "type": "function",
                        "function": {
                            "name": func["name"],
                            "arguments": _build_arguments(diag, str(last_user)),
                        },
                    },
                }
        return {"kind": "answer", "content": _compose_final_answer(messages)}
    # Skill 报告类请求识别：工作流把数据嵌入 Prompt（含"告警列表:"等标记），
    # 此时生成引用数据的结构化报告，让端到端演示输出更贴近真实效果。
    report = _compose_skill_report(str(last_user))
    if report:
        return {"kind": "answer", "content": report}
    # RAG 请求识别（模块 30）：Prompt 含"知识片段:"时生成引用式回答——
    # 引用 [来源 n] 编号是反幻觉的关键约束，Mock 行为与真实模型对齐
    rag_answer = _compose_rag_answer(str(last_user))
    if rag_answer:
        return {"kind": "answer", "content": rag_answer}
    content = (
        "我是网络运维助手（Mock LLM）。当前为离线模拟模式：我可以分析告警、"
        "查询切片 KPI、执行根因诊断。请描述你的问题，或提供告警编号。"
    )
    return {"kind": "answer", "content": content}


def _compose_skill_report(prompt: str) -> str | None:
    """从 Skill 组装的 Prompt 中提取数据段，生成结构化 Markdown 报告。"""
    import re as _re
    alarm_m = _re.search(r"告警列表: (\[.*?\])", prompt, _re.S)
    diag_m = _re.search(r"诊断结果: (\{.*?\}|未执行)", prompt, _re.S)
    kpi_m = _re.search(r"KPI: (\{.*?\})", prompt, _re.S)
    verdict_m = _re.search(r"(判定结果|审计结论): ([^\n]+)", prompt)
    load_m = _re.search(r"负载数据: (\[.*?\])", prompt, _re.S)
    device_m = _re.search(r"设备: (eNB-\d+)", prompt)
    if not (alarm_m or kpi_m or verdict_m or load_m):
        return None
    out: list[str] = []
    out.append("### 分析报告（Mock 结构化输出）")
    if device_m:
        out.append("**对象设备**: " + device_m.group(1))
    if alarm_m:
        try:
            alarms = json.loads(alarm_m.group(1))
            sev = {a.get("severity") for a in alarms} - {"minor"}
            top = "/".join(sorted(sev)) if sev else "minor"
            out.append("**告警概况**: 共 %d 条，最高级别 %s" % (len(alarms), top))
            for a in alarms[:3]:
                out.append("- [%s] %s %s (%s)" % (a.get("severity"), a.get("device_id"), a.get("text"), a.get("alarm_id")))
        except json.JSONDecodeError:
            pass
    if diag_m and diag_m.group(1) != "未执行":
        try:
            diag = json.loads(diag_m.group(1))
            abn = {k: v for k, v in diag.get("results", {}).items() if v != "normal"}
            if abn:
                out.append("**诊断发现**:")
                for k, v in abn.items():
                    out.append("- %s: %s" % (k, v))
            else:
                out.append("**诊断发现**: 各检查项正常")
        except json.JSONDecodeError:
            pass
    if load_m:
        try:
            loads = json.loads(load_m.group(1))
            for ld in loads:
                out.append("- 切片 %s: 负载率 %s（%s/%s Mbps）→ %s" % (
                    ld.get("slice_id"), ld.get("load_ratio"),
                    ld.get("throughput"), ld.get("capacity"), ld.get("level")))
        except json.JSONDecodeError:
            pass
    if kpi_m:
        try:
            kpi = json.loads(kpi_m.group(1))
            out.append("**KPI 概览**: 吞吐 %s Mbps / 时延 %s ms / 丢包 %s" % (kpi.get("throughput_mbps"), kpi.get("latency_ms"), kpi.get("packet_loss")))
        except json.JSONDecodeError:
            pass
    if verdict_m:
        out.append("**判定**: " + verdict_m.group(2).strip())
    out.append("**建议**: 1) 优先处置最高级别告警；2) 按诊断结果检查对应部件；3) 处理后观察 KPI 与告警清除情况。")
    return chr(10).join(out)

def _compose_rag_answer(prompt: str) -> str | None:
    """RAG 引用式回答：从 Prompt 的"知识片段"中抽取与问题相关的句子作答。
    确定性实现：按来源顺序引用，满足 [来源 n] 契约。"""
    if "知识片段" not in prompt:
        return None
    m = re.search(r"问题:\s*(.+)", prompt)
    question = m.group(1).strip() if m else ""
    # 抽取各来源的文本
    sources = re.findall(r"\[来源 (\d+)\] ([^\n]+)\n((?:.(?!\[来源))*?)(?=\n\n|\Z)",
                         prompt, re.S)
    if not sources:
        return None
    out = []
    for num, title, text in sources[:3]:
        # 从来源文本中取与问题关键词最相关的两句话（简单关键词重叠）
        q_terms = set(question) - set("的吗呢？? ")
        scored = []
        for sent in re.split(r"[。；\n]", text):
            sent = sent.strip()
            if len(sent) < 6:
                continue
            score = sum(1 for ch in q_terms if ch in sent)
            scored.append((score, sent))
        scored.sort(key=lambda x: -x[0])
        for _, sent in scored[:2]:
            out.append(f"[来源 {num}] {sent}。")
    if not out:
        return None
    return "根据知识库：" + "\n" + "\n".join(out[:4]) + "\n（以上内容均来自知识库检索片段，未使用外部信息。）"


def _chat_response_body(model: str, messages: list[dict], tools: list[dict]) -> dict:
    """组装 OpenAI 格式的非流式响应体。"""
    decision = _decide(messages, tools)
    prompt_tok = sum(_approx_tokens(str(m.get("content", ""))) for m in messages)
    if decision["kind"] == "tool_call":
        completion_tok = _approx_tokens(json.dumps(decision["tool_call"], ensure_ascii=False))
        message = {
            "role": "assistant",
            "content": None,
            "tool_calls": [decision["tool_call"]],
        }
        finish = "tool_calls"
        mode = "tool_call"
    else:
        completion_tok = _approx_tokens(decision["content"])
        message = {"role": "assistant", "content": decision["content"]}
        finish = "stop"
        mode = "chat"
    REQUEST_TOTAL.labels(model=model, mode=mode).inc()
    PROMPT_TOKENS.labels(model=model).inc(prompt_tok)
    COMPLETION_TOKENS.labels(model=model).inc(completion_tok)
    return {
        "id": f"chatcmpl-mock-{int(time.time() * 1000)}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "message": message, "finish_reason": finish}],
        "usage": {
            "prompt_tokens": prompt_tok,
            "completion_tokens": completion_tok,
            "total_tokens": prompt_tok + completion_tok,
        },
    }


def _stream_chat(model: str, messages: list[dict]):
    """OpenAI 流式协议（SSE）：逐段输出 content，最后发 usage 和 [DONE]。"""
    decision = _decide(messages, tools=[])
    text = decision.get("content", "")
    prompt_tok = sum(_approx_tokens(str(m.get("content", ""))) for m in messages)
    completion_tok = 0
    chunk_id = f"chatcmpl-mock-stream-{int(time.time() * 1000)}"
    pieces = [p for p in re.split(r"(?<=[。；，,.!?！？\s])", text) if p]
    for piece in pieces:
        completion_tok += _approx_tokens(piece)
        delta = {"choices": [{"index": 0, "delta": {"content": piece}, "finish_reason": None}]}
        yield f"data: {json.dumps({'id': chunk_id, 'object': 'chat.completion.chunk', 'created': int(time.time()), 'model': model, **delta}, ensure_ascii=False)}\n\n"
    final = {
        "id": chunk_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        "usage": {
            "prompt_tokens": prompt_tok,
            "completion_tokens": completion_tok,
            "total_tokens": prompt_tok + completion_tok,
        },
    }
    yield f"data: {json.dumps(final, ensure_ascii=False)}\n\ndata: [DONE]\n\n"
    REQUEST_TOTAL.labels(model=model, mode="chat_stream").inc()
    PROMPT_TOKENS.labels(model=model).inc(prompt_tok)
    COMPLETION_TOKENS.labels(model=model).inc(completion_tok)


def _mock_embedding(text: str, dim: int = 384) -> list[float]:
    """确定性伪 Embedding：字符级哈希累积。相同文本得到相同向量；
    非语义向量，仅用于让 RAG 流水线离线可跑（真实场景换 BGE 等模型）。"""
    vec = [0.0] * dim
    for ch in text:
        h = int(hashlib.md5(ch.encode("utf-8")).hexdigest(), 16)
        idx = h % dim
        sign = 1.0 if (h >> 8) % 2 == 0 else -1.0
        vec[idx] += sign
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [round(v / norm, 6) for v in vec]


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    """OpenAI 兼容的对话补全接口，支持 stream=true 的 SSE 流式返回。"""
    body = await request.json()
    model = body.get("model", "mock-gpt")
    messages: list[dict] = body.get("messages", [])
    tools: list[dict] = body.get("tools", []) or []
    INFLIGHT.inc()
    try:
        if body.get("stream"):
            return StreamingResponse(
                _stream_chat(model, messages),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache"},
            )
        return JSONResponse(_chat_response_body(model, messages, tools))
    finally:
        INFLIGHT.dec()


@app.post("/v1/embeddings")
async def embeddings(request: Request):
    """OpenAI 兼容的 Embedding 接口（确定性伪向量）。"""
    body = await request.json()
    model = body.get("model", "mock-embedding")
    inputs = body.get("input", [])
    if isinstance(inputs, str):
        inputs = [inputs]
    data = [
        {"object": "embedding", "index": i, "embedding": _mock_embedding(text)}
        for i, text in enumerate(inputs)
    ]
    REQUEST_TOTAL.labels(model=model, mode="embeddings").inc()
    tok = sum(_approx_tokens(t) for t in inputs)
    return {"object": "list", "data": data, "model": model,
            "usage": {"prompt_tokens": tok, "total_tokens": tok}}


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "service": "mock-llm"}


@app.get("/metrics")
async def metrics():
    # generate_latest() 返回 bytes：用 Response 一次性返回
    return Response(content=generate_latest(), media_type="text/plain; charset=utf-8")
