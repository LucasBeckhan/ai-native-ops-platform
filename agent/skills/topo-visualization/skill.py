#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/skills/topo-visualization/skill.py
# 所属模块: 追问第 3 轮 —— Skill 实现：网络拓扑可视化
# 说明:
#   - 工作流：查询各网元健康（诊断工具聚合）→ 查询防火墙封禁状态（跨
#     MCP Server 组合）→ 生成 ASCII 拓扑图（终端友好）+ Mermaid 源码
#     （贴 Grafana 注释/Markdown 报告）。
#   - 教学要点：可视化 Skill 的输出是"确定性渲染"（模板），LLM 只补充
#     异常标注的文字说明——图不能由 LLM 画（坐标会编造）。
#   - 多 MCP Server 组合：teleops（诊断）+ firewall（封禁状态），实测
#     验证 Harness 的多 Server 路由。
# =====================================================================
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../basics/m27-llm-adapter"))
from llm_adapter import ChatMessage, Role  # noqa: E402

DEVICES = ["eNB-1024", "eNB-1033", "eNB-1042"]
LINKS = [("eNB-1024", "eNB-1033", "X2"), ("eNB-1033", "eNB-1042", "S1")]


def _mermaid(dev_status: dict[str, str]) -> str:
    """Mermaid 源码：状态用节点着色语义（ok=绿/abnormal=红）。"""
    lines = ["graph LR"]
    for d in DEVICES:
        style = "🟢" if dev_status.get(d, "normal") == "normal" else "🔴"
        lines.append(f"  {d.replace('-', '')}[\"{style} {d}\"]")
    for a, b, link in LINKS:
        lines.append(f"  {a.replace('-', '')} --{link}--> {b.replace('-', '')}")
    return "\n".join(lines)


def _ascii(dev_status: dict[str, str]) -> str:
    def mark(d):
        return "OK " if dev_status.get(d, "normal") == "normal" else "!! "
    return (
        f"[{mark(DEVICES[0])}{DEVICES[0]}] ==X2==> [{mark(DEVICES[1])}{DEVICES[1]}]"
        f" ==S1==> [{mark(DEVICES[2])}{DEVICES[2]}]\n"
        f"      └─ 上联核心网（经防火墙策略组）"
    )


def run(ctx) -> str:
    diag_tool = ctx.router.resolve("run_diagnosis")
    status = {}
    for dev in DEVICES:
        if not diag_tool:
            break
        raw = ctx.router.execute(diag_tool, {"device_id": dev,
                                             "check_items": ["links", "cpu"]}, ctx.perm)
        try:
            results = json.loads(raw).get("results", {})
            status[dev] = "normal" if all(v == "normal" for v in results.values()) \
                else "abnormal"
        except json.JSONDecodeError:
            status[dev] = "unknown"

    # 跨 MCP Server 组合：防火墙封禁状态影响链路可用性标注
    blocked_note = ""
    fw_tool = ctx.router.resolve("list_blocked_ips")
    if fw_tool:
        raw = ctx.router.execute(fw_tool, {}, ctx.perm)
        try:
            n = json.loads(raw).get("total", 0)
            blocked_note = f"\n\n防火墙封禁规则 {n} 条（详见 firewall-mcp-server）。"
        except json.JSONDecodeError:
            pass

    abnormal = [d for d, s in status.items() if s != "normal"]
    note_prompt = (
        f"拓扑状态: {json.dumps(status, ensure_ascii=False)}。"
        f"异常设备: {abnormal or '无'}。{blocked_note}"
        " 请用三句话以内说明拓扑健康度与需要关注的设备，不要复述全部数据。"
    )
    resp = ctx.llm.chat([
        ChatMessage(Role.SYSTEM, "你是网管拓扑分析助手，输出极简中文说明。"),
        ChatMessage(Role.USER, note_prompt),
    ])
    ctx.facts["拓扑异常数"] = len(abnormal)
    header = ("**[Skill: topo-visualization v1.0.0]**\n\n```\n"
              + _ascii(status) + "\n```\n\n```mermaid\n"
              + _mermaid(status) + "\n```\n\n")
    return header + (resp.content or "")
