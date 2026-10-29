#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/skills/traffic-diagnosis/skill.py
# 所属模块: 模块 29 —— Skill 实现：切片流量诊断
# 说明:
#   - 工作流：提取切片ID → 经 Router 查询 KPI → 尝试经 MCP resources
#     读取 SLA 阈值（资源能力的教学演示：同一 Server 的 resources 与
#     tools 协同）→ 确定性阈值判定 → LLM 生成劣化分析。
#   - 阈值判定同样是代码而非 LLM（SLA 违规判定必须可复现）；
#     LLM 负责"劣化的可能原因与建议"这类开放性输出。
# =====================================================================
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../basics/m27-llm-adapter"))
from llm_adapter import ChatMessage, Role  # noqa: E402

# 兜底阈值（MCP resources 不可用时使用）
FALLBACK_SLA = {
    "eMBB": {"latency_ms": 20.0, "packet_loss": 0.005},
    "uRLLC": {"latency_ms": 10.0, "packet_loss": 0.001},
}


def _extract_slice(query: str) -> str:
    m = re.search(r"slice-[\w-]+", query)
    return m.group(0) if m else "slice-eMBB-001"


def _sla_from_mcp(router) -> dict | None:
    """演示 MCP resources 能力：从 Server 读取 SLA 阈值表。
    解析 '时延<=20ms 丢包<=0.5%' 形态的文本（教学用简单解析）。"""
    for handle in router.mcp_servers.values():
        try:
            text = handle.client.read_resource("teleops://knowledge/sla-thresholds")
            sla = {}
            for kind, lat, loss in re.findall(
                    r"(eMBB|uRLLC):\s*时延<=(\d+)ms\s*丢包<=([\d.]+)%", text):
                sla[kind] = {"latency_ms": float(lat),
                             "packet_loss": float(loss) / 100.0}
            return sla or None
        except Exception:                        # noqa: BLE001 —— resources 可选
            continue
    return None


def run(ctx) -> str:
    slice_id = _extract_slice(ctx.user_query)
    kpi_tool = ctx.router.resolve("query_kpis")
    if not kpi_tool:
        return "[traffic-diagnosis] 环境中缺少 query_kpis 工具"
    raw = ctx.router.execute(kpi_tool, {"slice_id": slice_id, "window": "last_1h"}, ctx.perm)
    try:
        kpi = json.loads(raw)
    except json.JSONDecodeError:
        return f"[traffic-diagnosis] KPI 结果不可解析: {raw[:100]}"

    sla = _sla_from_mcp(ctx.router) or FALLBACK_SLA
    kind = "uRLLC" if "uRLLC" in slice_id else "eMBB"
    threshold = sla.get(kind, FALLBACK_SLA["eMBB"])

    # ---- 确定性阈值判定 ----
    breaches = []
    if kpi.get("latency_ms", 0) > threshold["latency_ms"]:
        breaches.append(f"时延 {kpi['latency_ms']}ms > SLA {threshold['latency_ms']}ms")
    if kpi.get("packet_loss", 0) > threshold["packet_loss"]:
        breaches.append(f"丢包率 {kpi['packet_loss']} > SLA {threshold['packet_loss']}")
    verdict = "SLA 违规" if breaches else "SLA 正常"

    prompt = (
        "你是 5G 网络性能专家。基于以下数据输出劣化分析（Markdown）："
        "KPI 概览、SLA 判定、可能原因（如违规）、建议动作。数据未覆盖的原因不要编造。\n"
        f"切片: {slice_id}（类型 {kind}）\nKPI: {json.dumps(kpi, ensure_ascii=False)}\n"
        f"SLA阈值: {json.dumps(threshold, ensure_ascii=False)}\n"
        f"判定结果: {verdict}；违规项: {breaches or '无'}\n"
        f"用户问题: {ctx.user_query}"
    )
    resp = ctx.llm.chat([
        ChatMessage(Role.SYSTEM, "你是 5G 切片性能分析专家，输出简洁中文报告。"),
        ChatMessage(Role.USER, prompt),
    ])

    ctx.facts["最近诊断切片"] = slice_id
    ctx.facts["SLA判定"] = verdict
    header = f"**[Skill: traffic-diagnosis v1.0.0]** 切片 {slice_id} 判定: {verdict}\n\n"
    return header + (resp.content or "")
