#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/basics/m27-llm-adapter/llm_adapter.py
# 所属模块: 模块 27 —— 多厂商 LLM 统一适配层（生产级实现）
# 运行方式: 作为库被上层引用（模块 28/29）；demo 见本目录 demo_*.py
# 说明:
#   - 统一适配层解决四个生产问题：
#     ① 协议差异：OpenAI / Claude / 通义千问 的请求-响应格式不同，
#        统一收敛为内部 ChatMessage/ChatResponse 模型；
#     ② 失败治理：429/5xx 指数退避重试、连续失败熔断、备用模型降级；
#     ③ 流式输出：三种协议的 SSE 差异统一为"增量回调"接口；
#     ④ 成本核算：按模型价目表把 usage 换算成钱，累计上报（对接模块 33）。
#   - 设计原则：零第三方 SDK 依赖（纯 httpx），协议字段显式映射——
#     面试中"你了解各家协议差异吗"可以对照本文件逐字段回答。
#   - Mock LLM（OpenAI 协议兼容）是默认 provider；切换真实厂商只改配置。
# =====================================================================
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Iterator

import httpx


# ---------------------------------------------------------------------------
# 内部统一消息模型：上层代码只面向这三个类，不接触任何厂商特有字段
# ---------------------------------------------------------------------------
class Role(str, Enum):
    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"


@dataclass
class ChatMessage:
    role: Role
    content: str | None = None
    # Function Calling 字段（OpenAI 风格；其他协议由 provider 转换）
    tool_calls: list[dict] | None = None
    name: str | None = None
    tool_call_id: str | None = None


@dataclass
class Usage:
    prompt_tokens: int = 0
    completion_tokens: int = 0

    @property
    def total(self) -> int:
        return self.prompt_tokens + self.completion_tokens


@dataclass
class ChatResponse:
    content: str | None
    tool_calls: list[dict] | None
    finish_reason: str
    usage: Usage
    model: str            # 实际服务的模型（降级后可能与请求的不同）
    latency_ms: int
    cost_usd: float = 0.0


# ---------------------------------------------------------------------------
# 价目表（USD / 百万 Token，演示用数字；生产应从配置中心/厂商页面同步）
# ---------------------------------------------------------------------------
PRICE_TABLE_USD_PER_MTOK = {
    "mock-gpt": (0.0, 0.0),               # Mock 免费，用于成本链路验证
    "gpt-4o-mini": (0.15, 0.60),
    "gpt-4o": (2.50, 10.00),
    "claude-3-5-haiku": (0.80, 4.00),
    "qwen-plus": (0.55, 2.20),
}


class Provider(str, Enum):
    OPENAI = "openai"        # 含所有 OpenAI 协议兼容服务（Mock/DeepSeek/vLLM 等）
    CLAUDE = "claude"
    QWEN = "qwen"


@dataclass
class ProviderConfig:
    name: Provider
    base_url: str
    api_key_env: str                 # 从环境变量读 key，禁止硬编码（安全红线）
    model: str
    timeout_s: float = 30.0


# 三厂商的默认配置。base_url 可被环境变量覆盖（多环境/私有化部署）
DEFAULT_PROVIDERS: dict[str, ProviderConfig] = {
    "mock": ProviderConfig(Provider.OPENAI, "http://localhost:18000/v1", "MOCK_API_KEY", "mock-gpt"),
    "openai": ProviderConfig(Provider.OPENAI, "https://api.openai.com/v1", "OPENAI_API_KEY", "gpt-4o-mini"),
    "claude": ProviderConfig(Provider.CLAUDE, "https://api.anthropic.com/v1", "ANTHROPIC_API_KEY", "claude-3-5-haiku"),
    "qwen": ProviderConfig(Provider.QWEN, "https://dashscope.aliyuncs.com/compatible-mode/v1", "DASHSCOPE_API_KEY", "qwen-plus"),
}


class LLMError(Exception):
    """适配层统一异常：上层只需捕获这一种。"""


class LLMAdapter:
    """统一适配层主类。

    参数:
        providers: 有序 dict，key 为主备链路的槽位名（第一个是主力，
                   后面依次是降级备选——"多模型路由"的最小可用实现）。
    """

    def __init__(
        self,
        providers: dict[str, ProviderConfig] | None = None,
        max_retries: int = 3,
        backoff_base_s: float = 0.5,
        # 每次调用后的回调：记日志/上报 Prometheus（模块 33 接入点）
        on_usage: Callable[[str, Usage, float, int], None] | None = None,
    ):
        self._providers = providers or {"mock": DEFAULT_PROVIDERS["mock"]}
        self._max_retries = max_retries
        self._backoff = backoff_base_s
        self._on_usage = on_usage
        # 代理策略（生产环境的真实坑）：
        # macOS/Windows 的"系统代理"会被 httpx 自动拾取，连 localhost 请求也会
        # 被转发给代理，导致对 Mock LLM 的调用莫名 502/超时。因此：
        #   - 目标是内网/本机（Mock LLM、内网网关）→ trust_env=False 直连；
        #   - 目标是公网厂商 API → trust_env=True 尊重环境/系统代理。
        self._clients = {
            "direct": httpx.Client(timeout=30.0, trust_env=False),
            "proxied": httpx.Client(timeout=30.0, trust_env=True),
        }

    # ---------------- 对外主接口 ----------------
    def chat(
        self,
        messages: list[ChatMessage],
        tools: list[dict] | None = None,
        temperature: float = 0.2,
        stream_callback: Callable[[str], None] | None = None,
    ) -> ChatResponse:
        """按主备顺序尝试各 provider；单个 provider 内部做指数退避重试。
        stream_callback 非空时走流式协议，增量文本实时回调（打字机效果）。"""
        last_err: Exception | None = None
        for slot, cfg in self._providers.items():
            try:
                resp = self._chat_one_provider(cfg, messages, tools, temperature, stream_callback)
                return resp
            except LLMError as e:
                print(f"[adapter] 槽位 {slot}({cfg.model}) 失败: {e}，尝试下一个", flush=True)
                last_err = e
        raise LLMError(f"全部 {len(self._providers)} 个模型槽位均失败，最后错误: {last_err}")

    # ---------------- 单 provider 实现 ----------------
    def _chat_one_provider(
        self, cfg: ProviderConfig, messages: list[ChatMessage],
        tools: list[dict] | None, temperature: float,
        stream_callback: Callable[[str], None] | None,
    ) -> ChatResponse:
        # 429（限流）/ 5xx（服务端错）重试；4xx（参数错）不重试直接抛
        for attempt in range(self._max_retries + 1):
            start = time.time()
            try:
                if stream_callback is not None:
                    return self._stream_call(cfg, messages, temperature, stream_callback)
                return self._sync_call(cfg, messages, tools, temperature, start)
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                if status == 429 or status >= 500:
                    wait = self._backoff * (2 ** attempt)          # 指数退避：0.5/1/2s
                    if attempt < self._max_retries:
                        time.sleep(wait)
                        continue
                raise LLMError(f"{cfg.model} HTTP {status}: {e.response.text[:200]}") from e
            except httpx.RequestError as e:
                # 网络层错误（连接拒绝/超时）：重试后交给下一个 provider
                if attempt < self._max_retries:
                    time.sleep(self._backoff * (2 ** attempt))
                    continue
                raise LLMError(f"{cfg.model} 网络错误: {e}") from e
        raise LLMError("unreachable")   # 循环保证不落到这里，仅满足类型检查

    def _sync_call(self, cfg: ProviderConfig, messages: list[ChatMessage],
                   tools: list[dict] | None, temperature: float, start: float) -> ChatResponse:
        url, headers, body = self._build_request(cfg, messages, tools, temperature)
        client = self._client_for(cfg)
        r = client.post(url, headers=headers, json=body, timeout=cfg.timeout_s)
        r.raise_for_status()
        data = r.json()
        # 三种协议的响应归一化（差异点逐行注释，面试考点）
        if cfg.name == Provider.CLAUDE:
            # Claude: content 是块列表；tool_use 也是 content 块的一种
            blocks = data["content"]
            content = "".join(b.get("text", "") for b in blocks if b["type"] == "text")
            tool_calls = [
                {"id": b["id"], "type": "function", "function": {
                    "name": b["name"], "arguments": json.dumps(b.get("input", {}), ensure_ascii=False)}}
                for b in blocks if b["type"] == "tool_use"
            ] or None
            usage = Usage(data["usage"]["input_tokens"], data["usage"]["output_tokens"])
            finish = data.get("stop_reason", "stop")
        else:
            # OpenAI / Qwen(compatible-mode): choices[0].message 结构一致
            msg = data["choices"][0]["message"]
            content = msg.get("content")
            tool_calls = msg.get("tool_calls")
            usage = Usage(data["usage"]["prompt_tokens"], data["usage"]["completion_tokens"])
            finish = data["choices"][0].get("finish_reason", "stop")
        resp = ChatResponse(
            content=content, tool_calls=tool_calls, finish_reason=finish,
            usage=usage, model=data.get("model", cfg.model),
            latency_ms=int((time.time() - start) * 1000),
        )
        resp.cost_usd = self._cost_of(resp.model, usage)
        if self._on_usage:
            self._on_usage(resp.model, usage, resp.cost_usd, resp.latency_ms)
        return resp

    def _stream_call(self, cfg: ProviderConfig, messages: list[ChatMessage],
                     temperature: float, cb: Callable[[str], None]) -> ChatResponse:
        """SSE 流式：逐行解析 data: 载荷，增量文本回调；结束聚合 usage。
        三协议流式格式不同：OpenAI 系 delta.content；Claude 是
        content_block_delta 事件。此处覆盖 OpenAI 系（Mock/Qwen 兼容模式）。"""
        url, headers, body = self._build_request(cfg, messages, None, temperature)
        body["stream"] = True
        chunks: list[str] = []
        start = time.time()
        usage = Usage()
        with self._client_for(cfg).stream("POST", url, headers=headers, json=body,
                                          timeout=cfg.timeout_s) as r:
            r.raise_for_status()
            for line in r.iter_lines():
                if not line.startswith("data:"):
                    continue
                payload = line[len("data:"):].strip()
                if payload == "[DONE]":
                    break
                evt = json.loads(payload)
                # OpenAI 流式的 usage 在最后一个 chunk（stream_options 控制），
                # Mock 实现放在 finish chunk；做双兼容
                if "usage" in evt and evt["usage"]:
                    usage = Usage(evt["usage"]["prompt_tokens"], evt["usage"]["completion_tokens"])
                delta = evt["choices"][0].get("delta", {})
                piece = delta.get("content") or ""
                if piece:
                    chunks.append(piece)
                    cb(piece)                      # 增量回调：UI 打字机效果的接口
        resp = ChatResponse(
            content="".join(chunks), tool_calls=None, finish_reason="stop",
            usage=usage, model=cfg.model,
            latency_ms=int((time.time() - start) * 1000),
        )
        resp.cost_usd = self._cost_of(cfg.model, usage)
        if self._on_usage:
            self._on_usage(cfg.model, usage, resp.cost_usd, resp.latency_ms)
        return resp

    def _client_for(self, cfg: ProviderConfig) -> httpx.Client:
        """按目标地址选择直连或走系统代理的客户端。"""
        from urllib.parse import urlparse
        host = urlparse(cfg.base_url).hostname or ""
        local = host in ("localhost", "127.0.0.1", "::1") or host.endswith(".local")
        # 私网段（容器网络 172.x/10.x/192.168.x）同样直连
        if host.startswith(("10.", "172.", "192.168.")):
            local = True
        return self._clients["direct"] if local else self._clients["proxied"]

    def _build_request(self, cfg: ProviderConfig, messages: list[ChatMessage],
                       tools: list[dict] | None, temperature: float):
        """把统一消息模型映射为各厂商的请求格式（协议差异集中在这里）。"""
        import os
        api_key = os.environ.get(cfg.api_key_env, "mock-key")   # Mock 无需真实 key
        headers = {"Content-Type": "application/json"}
        if cfg.name == Provider.CLAUDE:
            url = f"{cfg.base_url}/messages"
            headers.update({"x-api-key": api_key, "anthropic-version": "2023-06-01"})
            body: dict = {
                "model": cfg.model, "max_tokens": 1024, "temperature": temperature,
                "messages": [
                    # Claude 把 system 独立为顶层参数；tool 结果用 tool_result 块
                    *[{"role": "user", "content": m.content or ""}
                      if m.role == Role.SYSTEM else
                      {"role": m.role.value, "content": m.content or ""}
                      for m in messages]
                ],
            }
            # 注意：Claude 的 system 需要单独字段，这里教学实现做了简化映射；
            # 严格实现见 docs/01-llm-adapter.md 的协议对照表。
            if messages and messages[0].role == Role.SYSTEM:
                body["system"] = messages[0].content
                body["messages"] = body["messages"][1:]
        else:
            url = f"{cfg.base_url}/chat/completions"
            headers["Authorization"] = f"Bearer {api_key}"
            body = {
                "model": cfg.model, "temperature": temperature,
                "messages": [
                    {"role": m.role.value, "content": m.content,
                     **({"tool_calls": m.tool_calls} if m.tool_calls else {})
                     } if m.role != Role.TOOL else
                    {"role": "tool", "content": m.content,
                     "tool_call_id": m.tool_call_id, "name": m.name}
                    for m in messages
                ],
            }
            if tools:
                body["tools"] = tools
        return url, headers, body

    @staticmethod
    def _cost_of(model: str, usage: Usage) -> float:
        """按价目表换算美元成本；未知模型按 0 记（日志告警而非中断）。"""
        in_p, out_p = PRICE_TABLE_USD_PER_MTOK.get(model, (0.0, 0.0))
        return usage.prompt_tokens / 1e6 * in_p + usage.completion_tokens / 1e6 * out_p


def extract_json(text: str) -> dict:
    """从 LLM 输出中提取 JSON：剥 markdown 代码围栏 → 定位首尾大括号。
    模块 26 的"结构化输出契约"在生产上的最后一道工序——真实模型
    即使被约束也可能输出 ```json 包裹或前后带解释文字。"""
    s = text.strip()
    if s.startswith("```"):
        s = s.split("```")[1] if "```" in s else s
        if s.startswith("json"):
            s = s[4:]
        s = s.strip()
    start, end = s.find("{"), s.rfind("}")
    if start == -1 or end == -1:
        raise ValueError(f"输出中未找到 JSON: {text[:120]}")
    return json.loads(s[start:end + 1])
