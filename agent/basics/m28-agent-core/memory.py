#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/basics/m28-agent-core/memory.py
# 所属模块: 模块 28 —— 三层 Memory 系统（短期/长期/工作记忆）
# 运行方式: 作为库被 react_agent.py 引用；单独运行可自检
# 说明:
#   - 短期记忆（Conversation Buffer）：当前会话消息窗口，按 Token 预算
#     裁剪——超限时保留 system + 最早的一条 + 最近 N 条（保头保尾策略，
#     因为任务目标在开头、最近上下文在结尾）。
#   - 长期记忆（Vector Store）：跨会话的事实沉淀。本项目用 Mock LLM 的
#     /v1/embeddings（确定性伪向量）+ 进程内余弦检索实现，接口设计与
#     Milvus/Chroma 一致——模块 30 换 Milvus 时只换实现不动调用方。
#   - 工作记忆（Scratchpad）：一次 ReAct 循环内的中间观察记录，
#     会话结束即丢弃——对应"草稿纸"。
#   - 面试高频：为什么不能把所有历史都塞上下文？（成本线性涨 + 干扰
#     注意力 + 窗口上限）=> 分层记忆是 Agent 工程的必选项而非优化项。
# =====================================================================
import json
import time
import urllib.request
from dataclasses import dataclass, field


def _approx_tokens(text: str) -> int:
    """与 Mock LLM 同口径的 Token 估算（中文按字、英文按 4 字符）。"""
    if not text:
        return 0
    cjk = sum(1 for ch in text if "\u4e00" <= ch <= "\u9fff")
    return cjk + max(1, (len(text) - cjk) // 4)


@dataclass
class Message:
    role: str
    content: str
    name: str | None = None      # tool 角色消息记录工具名（供上游摘要模板识别）


class ShortTermMemory:
    """会话内消息缓冲，带 Token 预算裁剪。"""

    def __init__(self, max_tokens: int = 2000):
        self.max_tokens = max_tokens
        self.messages: list[Message] = []

    def add(self, role: str, content: str, name: str | None = None) -> None:
        self.messages.append(Message(role, content, name))
        self._trim()

    def _trim(self) -> None:
        """超预算时保头（system/目标）保尾（最近上下文），丢中间。"""
        total = sum(_approx_tokens(m.content) for m in self.messages)
        while total > self.max_tokens and len(self.messages) > 3:
            dropped = self.messages.pop(1)   # 保住第 0 条（system/任务目标）
            total -= _approx_tokens(dropped.content)

    def as_openai_messages(self) -> list[dict]:
        out = []
        for m in self.messages:
            msg = {"role": m.role, "content": m.content}
            if m.role == "tool" and m.name:
                msg["name"] = m.name
            out.append(msg)
        return out


class LongTermMemory:
    """跨会话向量记忆：add(文本) → embed → 入库；search(问题) → top-k。
    用 Mock LLM 的 /v1/embeddings（确定性伪向量）；接口与 Chroma 对齐。"""

    def __init__(self, base_url: str = "http://localhost:18000/v1", dim: int = 384):
        self.base_url = base_url
        self.dim = dim
        self.records: list[dict] = []   # {"text", "vector", "ts", "meta"}

    def _embed(self, text: str) -> list[float]:
        body = json.dumps({"model": "mock-embedding", "input": text}).encode()
        req = urllib.request.Request(
            f"{self.base_url}/embeddings", data=body,
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.load(r)["data"][0]["embedding"]

    def add(self, text: str, meta: dict | None = None) -> None:
        self.records.append({
            "text": text, "vector": self._embed(text),
            "ts": time.time(), "meta": meta or {},
        })

    def search(self, query: str, k: int = 3) -> list[dict]:
        if not self.records:
            return []
        qv = self._embed(query)
        scored = []
        for rec in self.records:
            sim = sum(a * b for a, b in zip(qv, rec["vector"]))   # 已归一化，点积=余弦
            scored.append((sim, rec))
        scored.sort(key=lambda x: -x[0])
        return [{"score": round(s, 4), "text": r["text"], "meta": r["meta"]}
                for s, r in scored[:k]]


@dataclass
class Scratchpad:
    """工作记忆：一次 ReAct 执行中的 Thought/Action/Observation 草稿。"""
    entries: list[tuple[str, str]] = field(default_factory=list)

    def note(self, kind: str, text: str) -> None:
        self.entries.append((kind, text))

    def render(self) -> str:
        return "\n".join(f"[{k}] {t}" for k, t in self.entries)


if __name__ == "__main__":
    # 自检一：短期记忆裁剪
    stm = ShortTermMemory(max_tokens=50)
    stm.add("system", "你是网络运维助手，当前任务：分析 eNB-1024 告警并给出处置建议。")
    for i in range(20):
        stm.add("user", f"补充信息第 {i} 条：这是一段用于撑爆 Token 预算的填充文本" * 3)
    msgs = stm.as_openai_messages()
    print(f"裁剪后消息数: {len(msgs)}（首条保留: {msgs[0]['content'][:18]}...）")

    # 自检二：长期记忆写入与检索（需 Mock LLM）
    ltm = LongTermMemory()
    ltm.add("eNB-1024 于 10 月 12 日发生光模块故障，更换尾纤后恢复。", {"case": "case-1012"})
    ltm.add("slice-uRLLC-001 的时延 SLA 是 10ms，超出即违规。", {"case": "sla-urllc"})
    hits = ltm.search("eNB-1024 光功率异常怎么处理")
    print("长期记忆检索 top1:", hits[0]["text"][:40], f"(score={hits[0]['score']})")

    # 自检三：工作记忆
    pad = Scratchpad()
    pad.note("Thought", "先查该设备告警")
    pad.note("Action", "query_alarms(device_id=eNB-1024)")
    pad.note("Observation", "2 条告警：小区退服 + 光模块异常")
    print("工作记忆:\n" + pad.render())
