#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/basics/rag/rag_pipeline.py
# 所属模块: 模块 30 —— 运维知识库 RAG 完整实现（可实测）
# 运行方式: python3 rag_pipeline.py    （需 Mock LLM :18000）
# 说明:
#   - 模块 30 五段流水线的可运行实现：
#       分块（递归分块，带 chunk_size/overlap）
#         → 向量化（Mock 确定性向量，接口与真实 embedding 一致）
#         → 检索（向量检索 + 关键词 BM25 近似 + 混合融合 RRF）
#         → 生成（引用式回答：答案必须带 [来源 n] 编号）
#         → 评估（检索命中率的 mini 评估集）
#   - 知识库内容：通信运维手册（光模块/SYN 攻击/切片 SLA）——
#     与课程各场景呼应，检索效果可人工核验。
#   - 生产对照（注释标注）：真实 embedding（BGE/text-embedding）、
#     向量库（Milvus/Chroma，本项目 LongTermMemory 接口对齐）、
#     Reranker（交叉编码器）、RAGAS 评估。
# =====================================================================
import json
import math
import os
import sys
import re
import urllib.request
from collections import Counter
from typing import Callable

LLM_BASE = os.environ.get("LLM_BASE_URL", "http://localhost:18000/v1")

# ---------------------------------------------------------------------------
# 知识库（生产：从文档目录加载；这里内联保证实验自包含）
# ---------------------------------------------------------------------------
KNOWLEDGE_DOCS = {
    "手册-光模块": (
        "光模块接收异常处置流程：\n"
        "1) 确认收光功率：读 OTP，正常范围 -12 至 -24 dBm，低于 -24 dBm 判定为异常。\n"
        "2) 检查尾纤：弯折半径不得小于 3cm，接头端面用无尘纸+无水酒精清洁。\n"
        "3) 更换光模块：优先更换对端后再换本端，更换后观察 10 分钟收光是否恢复。\n"
        "4) 升级传输组：若本端对端均正常仍劣化，提交传输组核查中间链路衰减。\n"
        "常见根因：尾纤弯折（占 60%）、端面污染（占 25%）、光模块老化（占 15%）。"
    ),
    "手册-DDoS处置": (
        "SYN Flood 攻击处置流程：\n"
        "1) 确认特征：纯 SYN 包占比超过 80%，单源或分布式高速发往集中端口。\n"
        "2) 采集侧限流：上联口开启 CAR，SYN 速率限制到基线的 1.5 倍。\n"
        "3) 下发封禁：对确认攻击源 IP 配置 ACL drop，默认时长 30 分钟自动解除。\n"
        "4) 旁路牵引：攻击流量超过防护阈值时切换黑洞路由，需双人复核。\n"
        "5) 事后溯源：保留 7 天 NetFlow 用于攻击画像与报告。"
    ),
    "手册-切片SLA": (
        "5G 切片 SLA 阈值定义：\n"
        "eMBB 切片：时延 ≤ 20ms，丢包率 ≤ 0.5%，吞吐 ≥ 700Mbps。\n"
        "uRLLC 切片：时延 ≤ 10ms，丢包率 ≤ 0.1%，可靠性 99.999%。\n"
        "mMTC 切片：时延 ≤ 100ms，连接数密度每平方公里 100 万。\n"
        "违规判定：连续两个 30 秒窗口超阈值即记一次 SLA 违规；违规处置优先级"
        "按用户数×业务等级排序。"
    ),
    "手册-基站重启": (
        "基站远程重启操作规范：\n"
        "1) 重启前确认：避开业务忙时（工作日 9-11 点、19-23 点）。\n"
        "2) 重启顺序：先 BBU 后 RRU；重启间隔不低于 5 分钟。\n"
        "3) 影响评估：单站重启影响该站全部在网用户约 3-5 分钟。\n"
        "4) 权限要求：远程重启属于高危操作，需工单审批 + 值班经理确认。\n"
        "5) 重启后验证：检查小区状态、RRC 建立成功率恢复到 99% 以上。"
    ),
}


# ---------------------------------------------------------------------------
# 第一段：递归分块（chunk_size + overlap；保留句子边界）
# ---------------------------------------------------------------------------
def recursive_chunk(text: str, chunk_size: int = 120, overlap: int = 20) -> list[str]:
    """按字符窗滑切，窗口边界对齐句号/换行（递归分块的简化教学版；
    生产用 langchain RecursiveCharacterTextSplitter 或语义分块）。"""
    chunks, start = [], 0
    text = re.sub(r"\n+", "\n", text.strip())
    while start < len(text):
        end = min(start + chunk_size, len(text))
        # 边界对齐：在窗口末尾向前找最近的句子边界
        if end < len(text):
            for sep in ("。", "；", "\n"):
                cut = text.rfind(sep, start, end)
                if cut > start + chunk_size // 2:
                    end = cut + 1
                    break
        chunks.append(text[start:end].strip())
        start = max(end - overlap, start + 1)   # overlap 保证跨块信息连续
    return [c for c in chunks if len(c) > 10]


# ---------------------------------------------------------------------------
# 第二段：向量化（Mock 确定性向量；生产换 BGE/OpenAI embedding）
# ---------------------------------------------------------------------------
def _llm_post(path: str, body: dict) -> dict:
    req = urllib.request.Request(
        f"{LLM_BASE}{path}", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)


def embed(texts: list[str]) -> list[list[float]]:
    resp = _llm_post("/embeddings", {"model": "mock-embedding", "input": texts})
    data = sorted(resp["data"], key=lambda d: d["index"])
    return [d["embedding"] for d in data]


def cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))   # 向量已归一化


# ---------------------------------------------------------------------------
# 第三段：混合检索（向量 + 关键词 RRF 融合）—— 模块 30 的核心考点
# ---------------------------------------------------------------------------
class HybridRetriever:
    """Dense（向量）+ Sparse（关键词）双通道，RRF（Reciprocal Rank Fusion）
    融合排名。为什么混合：向量管语义（"收光差"≈"光功率异常"），
    关键词管精确（"eNB-1024" 这类术语），单通道各有盲区。"""

    def __init__(self, docs: dict[str, str], chunk_size: int = 120):
        self.chunks: list[dict] = []   # {"id", "doc", "text", "vector", "tokens"}
        for doc_name, text in docs.items():
            for i, chunk in enumerate(recursive_chunk(text, chunk_size)):
                self.chunks.append({"id": f"{doc_name}#{i}", "doc": doc_name, "text": chunk})
        # 批量向量化（一次请求，生产注意批量大小限制）
        vectors = embed([c["text"] for c in self.chunks])
        for c, v in zip(self.chunks, vectors):
            c["vector"] = v
            c["tokens"] = Counter(self._tokenize(c["text"]))

    @staticmethod
    def _tokenize(text: str) -> list[str]:
        # 极简分词：按标点切 + 2-gram（中文检索的教学近似；生产用 jieba/BM25 库）
        parts = re.split(r"[，。；：\s（）()]", text)
        terms = []
        for p in parts:
            if len(p) >= 2:
                terms.append(p)
                terms += [p[i:i + 2] for i in range(len(p) - 1)]   # 2-gram
        return [t for t in terms if t]

    def dense_search(self, query: str, k: int = 5) -> list[tuple[int, float]]:
        qv = embed([query])[0]
        scored = sorted(((i, cosine(qv, c["vector"])) for i, c in enumerate(self.chunks)),
                        key=lambda x: -x[1])
        return scored[:k]

    def sparse_search(self, query: str, k: int = 5) -> list[tuple[int, float]]:
        q_terms = Counter(self._tokenize(query))
        scored = []
        for i, c in enumerate(self.chunks):
            score = sum(min(cnt, c["tokens"].get(t, 0)) for t, cnt in q_terms.items())
            scored.append((i, score))
        scored.sort(key=lambda x: -x[1])
        return [(i, s) for i, s in scored[:k] if s > 0]

    @staticmethod
    def _rrf(*rankings: list[int], k: int = 60) -> list[int]:
        """Reciprocal Rank Fusion：每通道按名次给 1/(k+rank) 分，累加。
        比加权分数融合更稳（不同通道分数量纲不同）。"""
        scores: dict[int, float] = {}
        for ranking in rankings:
            for rank, idx in enumerate(ranking):
                scores[idx] = scores.get(idx, 0.0) + 1.0 / (k + rank + 1)
        return [i for i, _ in sorted(scores.items(), key=lambda x: -x[1])]

    def search(self, query: str, k: int = 3) -> list[dict]:
        dense = [i for i, _ in self.dense_search(query, k * 2)]
        sparse = [i for i, _ in self.sparse_search(query, k * 2)]
        fused = self._rrf(dense, sparse)[:k]
        return [{"id": self.chunks[i]["id"], "doc": self.chunks[i]["doc"],
                 "text": self.chunks[i]["text"]} for i in fused]


# ---------------------------------------------------------------------------
# 第四段：生成（引用式回答——答案必须带 [来源 n]，反幻觉的最直接手段）
# ---------------------------------------------------------------------------
def generate(retriever: HybridRetriever, question: str, llm_post: Callable) -> str:
    hits = retriever.search(question, k=3)
    context = "\n\n".join(f"[来源 {i+1}] {h['id']}\n{h['text']}"
                          for i, h in enumerate(hits))
    prompt = (
        "你是网络运维知识助手。只依据以下知识片段回答问题；"
        "每个论断标注 [来源 n]；片段不足时如实说明。\n\n"
        f"知识片段:\n{context}\n\n问题: {question}"
    )
    resp = llm_post("/chat/completions",
                    {"model": "mock-gpt", "temperature": 0.1,
                     "messages": [{"role": "user", "content": prompt}]})
    answer = resp["choices"][0]["message"]["content"]
    return f"检索命中: {[h['id'] for h in hits]}\n\n{answer}"


# ---------------------------------------------------------------------------
# 第五段：评估（检索命中的 mini 评估集——RAGAS 思想的最小实现）
# ---------------------------------------------------------------------------
EVAL_SET = [
    # (问题, 期望命中的知识文档)
    ("光功率 -28dBm 怎么处理？", "手册-光模块"),
    ("遇到 SYN 攻击第一步做什么？", "手册-DDoS处置"),
    ("uRLLC 切片的时延要求是多少？", "手册-切片SLA"),
    ("远程重启基站要注意什么？", "手册-基站重启"),
    ("尾纤弯折的占比是多少？", "手册-光模块"),
]


def evaluate(retriever: HybridRetriever) -> float:
    hits = 0
    print("== 检索命中率评估 ==")
    for q, expect in EVAL_SET:
        result = retriever.search(q, k=3)
        got = {r["doc"] for r in result}
        ok = expect in got
        hits += ok
        print(f"  {'✓' if ok else '✗'} {q} → 期望 {expect}，实际 {sorted(got)}")
    rate = hits / len(EVAL_SET)
    print(f"  检索命中率: {hits}/{len(EVAL_SET)} = {rate:.0%}")
    return rate


def main() -> None:
    print("== 构建知识库（分块 + 向量化） ==")
    retriever = HybridRetriever(KNOWLEDGE_DOCS)
    print(f"  4 篇手册 → {len(retriever.chunks)} 个块")

    print("\n== 混合检索演示（向量管语义 + 关键词管精确） ==")
    for q in ("收光很差怎么排查",                     # 语义改写：不含手册原词
              "SLA 违规连续几个窗口判定一次",          # 精确细节
              "重启基站的审批要求"):                   # 跨句信息
        hits = retriever.search(q, k=2)
        print(f"  Q: {q}")
        for h in hits:
            print(f"    → {h['id']}: {h['text'][:40]}...")

    print("\n== 生成（引用式回答） ==")
    print(generate(retriever, "光功率 -28dBm 怎么处理？", _llm_post)[:400])

    evaluate(retriever)


if __name__ == "__main__":
    main()
