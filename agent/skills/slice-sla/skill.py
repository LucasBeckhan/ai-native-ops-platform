#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/skills/slice-sla/skill.py
# 所属模块: 场景一 —— "切片 SLA 分析 Skill"（K8s+大数据+Agent 融合的智能层）
# 说明:
#   - 工作流（确定性优先，LLM 只做综合）：
#     ① 经 MCP 工具 es_query_alerts 拉取最近告警（Flink KPI 告警/DDoS 告警）
#     ② 经 MCP 工具 query_kpis 拉取切片实时 KPI
#     ③ 代码判定 SLA 违规（阈值来自 MCP resources，判定可审计）
#     ④ LLM 生成 SLA 违规报告（引用证据编号）
#   - 这是文档所说"Skill 封装 KPI 查询 + 异常检测 + 报告生成完整流程"的实现。
#   - 与 traffic-diagnosis 的差异：本 Skill 面向"周期性 SLA 巡检报告"场景，
#     数据面走 ES（Flink 管道的产出），而 traffic-diagnosis 面向"单次故障诊断"。
# =====================================================================
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../basics/m27-llm-adapter"))
from llm_adapter import ChatMessage, Role  # noqa: E402

SLA_DEFAULT = {
    "slice-eMBB-001": {"latency_ms": 20.0, "packet_loss": 0.005},
    "slice-uRLLC-001": {"latency_ms": 10.0, "packet_loss": 0.001},
    "slice-mMTC-001": {"latency_ms": 100.0, "packet_loss": 0.01},
}


def _extract_slice(query: str) -> str | None:
    m = re.search(r"slice-[\w-]+", query)
    return m.group(0) if m else None


def _sla_from_mcp(router) -> dict:
    for handle in router.mcp_servers.values():
        try:
            text = handle.client.read_resource("teleops://knowledge/sla-thresholds")
            sla = {}
            for kind, lat, loss in re.findall(
                    r"(eMBB|uRLLC|mMTC):\s*时延<=(\d+)ms\s*丢包<=([\d.]+)%", text):
                sla[f"slice-{kind}-001"] = {
                    "latency_ms": float(lat), "packet_loss": float(loss) / 100.0}
            if sla:
                return sla
        except Exception:                        # noqa: BLE001 —— resources 可选
            continue
    return SLA_DEFAULT


def run(ctx) -> str:
    target = _extract_slice(ctx.user_query)

    # ---- ① 拉取最近告警（ES 数据面，经 MCP）----
    es_tool = ctx.router.resolve("es_query_alerts")
    alerts = {"total": 0, "items": []}
    if es_tool:
        args = {"size": 20}
        if target:
            args["keyword"] = target.replace("slice-", "")   # type/keyword 匹配
        raw = ctx.router.execute(es_tool, args, ctx.perm)
        try:
            alerts = json.loads(raw)
        except json.JSONDecodeError:
            alerts = {"total": 0, "items": [], "raw": raw[:120]}

    # ---- ② 拉取切片 KPI ----
    slices = [target] if target else list(SLA_DEFAULT)
    kpis = {}
    kpi_tool = ctx.router.resolve("query_kpis")
    if kpi_tool:
        for sid in slices:
            raw = ctx.router.execute(kpi_tool, {"slice_id": sid, "window": "last_1h"}, ctx.perm)
            try:
                kpis[sid] = json.loads(raw)
            except json.JSONDecodeError:
                pass

    # ---- ③ SLA 判定（代码规则，可审计）----
    sla = _sla_from_mcp(ctx.router)
    violations = []
    for sid, kpi in kpis.items():
        th = sla.get(sid, {})
        if kpi.get("latency_ms", 0) > th.get("latency_ms", float("inf")):
            violations.append(f"{sid}: 时延 {kpi['latency_ms']}ms > SLA {th.get('latency_ms')}ms")
        if kpi.get("packet_loss", 0) > th.get("packet_loss", float("inf")):
            violations.append(f"{sid}: 丢包 {kpi['packet_loss']} > SLA {th.get('packet_loss')}")
    verdict = "存在 SLA 违规" if violations else "全部切片 SLA 达标"

    # ---- ④ LLM 报告（引用证据）----
    prompt = (
        "你是 5G 网络运维专家。基于以下数据输出 SLA 巡检报告（Markdown）：\n"
        "1. 总体判定 2. 违规明细（引用数据）3. 告警关联（引用 alarm 条目）"
        "4. 处置建议。数据未覆盖的信息不要编造。\n\n"
        f"判定结果: {verdict}\n违规明细: {violations or '无'}\n"
        f"实时 KPI: {json.dumps(kpis, ensure_ascii=False)}\n"
        f"最近告警({alerts.get('total', 0)} 条): "
        f"{json.dumps(alerts.get('items', [])[:5], ensure_ascii=False)}\n"
        f"用户问题: {ctx.user_query}"
    )
    resp = ctx.llm.chat([
        ChatMessage(Role.SYSTEM, "你是 5G 切片 SLA 分析专家，输出紧凑专业的中文报告。"),
        ChatMessage(Role.USER, prompt),
    ])

    ctx.facts["SLA巡检结论"] = verdict
    header = (f"**[Skill: slice-sla v1.0.0]** 巡检范围: {', '.join(slices)} → {verdict}\n\n"
              f"- ES 告警: {alerts.get('total', 0)} 条\n- 违规项: {len(violations)}\n\n")
    return header + (resp.content or "")


if __name__ == "__main__":
    # 独立自检：构造最小 ctx（Mock LLM + 本地工具 + 已运行的 MCP Server）
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../harness/src"))
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../basics/m28-agent-core"))
    from tool_router import new_router                     # noqa: E402
    from permission import PermissionManager               # noqa: E402
    from llm_adapter import LLMAdapter                     # noqa: E402

    router = new_router(use_mcp=True, mcp_command=[
        sys.executable, os.path.join(os.path.dirname(__file__),
                                     "../../mcp/minimal/mcp_server.py")])
    perm = PermissionManager(session_id="s-sla", user_id="selftest", role="engineer")

    class Ctx:                                             # 最小 SkillContext 替身
        session_id = "s-sla"
        user_query = "给我出一份 slice-uRLLC-001 的 SLA 巡检报告"
        router, llm, perm, facts = router, LLMAdapter(), perm, {}

    print(run(Ctx())[:300])
    router.close()
