#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/skills/alarm-analysis/skill.py
# 所属模块: 模块 29 —— Skill 实现：网络告警分析
# 说明:
#   - Skill = 确定性工作流 + 少量 LLM 判断。本 Skill 的五步工作流：
#       ① 提取设备ID（正则，不需要 LLM）
#       ② 经 Router 查询告警（数据面）
#       ③ 有告警且用户追问原因 → 经 Router 运行诊断（数据面）
#       ④ 组装结构化上下文 → LLM 生成结论（判断面）
#       ⑤ 写回会话事实，返回报告
#   - 关键设计：数据获取全部走 Router（权限/审计强制路径），
#     LLM 只做"综合判断"不做"取数"——幻觉面最小化的领域封装。
#   - 对比实验注记：本文件不 import 任何工具实现，工具经由
#     router.resolve() 解析为本地函数或 MCP 调用——两种接入方式下
#     本文件零改动。
# =====================================================================
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../basics/m27-llm-adapter"))
from llm_adapter import ChatMessage, Role  # noqa: E402


def _extract_device(query: str) -> str | None:
    m = re.search(r"eNB-\d{4}", query)
    return m.group(0) if m else None


def run(ctx) -> str:
    device = _extract_device(ctx.user_query)

    # ---- 步骤②：查告警（Router 自动解析本地/MCP，含权限裁决）----
    alarm_tool = ctx.router.resolve("query_alarms")
    if not alarm_tool:
        return "[alarm-analysis] 环境中缺少 query_alarms 工具"
    alarms_raw = ctx.router.execute(
        alarm_tool, {"device_id": device} if device else {}, ctx.perm)
    try:
        alarms = json.loads(alarms_raw)
    except json.JSONDecodeError:
        alarms = []

    if not alarms:
        return f"[alarm-analysis] {device or '全网'} 当前无告警记录。"

    # ---- 步骤③：追问原因/建议时追加诊断 ----
    diagnosis = None
    if any(w in ctx.user_query for w in ["原因", "建议", "诊断", "根因"]):
        diag_tool = ctx.router.resolve("run_diagnosis")
        if diag_tool and device:
            diag_raw = ctx.router.execute(diag_tool, {"device_id": device}, ctx.perm)
            try:
                diagnosis = json.loads(diag_raw)
            except json.JSONDecodeError:
                diagnosis = {"raw": diag_raw}

    # ---- 步骤④：LLM 综合判断（内联精简版五段式 Prompt）----
    severity_rank = {"critical": 0, "major": 1, "minor": 2}
    top = sorted(alarms, key=lambda a: severity_rank.get(a.get("severity", "minor"), 3))[0]
    prompt = (
        "你是网络运维专家。基于以下真实数据输出分析报告（Markdown），"
        "包含：告警概况、根因假设（引用诊断结果）、处置建议（按优先级）、"
        "风险提示。不要编造数据中没有的信息。\n\n"
        f"设备: {device or '多设备'}\n"
        f"告警列表: {json.dumps(alarms, ensure_ascii=False)}\n"
        f"诊断结果: {json.dumps(diagnosis, ensure_ascii=False) if diagnosis else '未执行'}\n"
        f"用户问题: {ctx.user_query}"
    )
    resp = ctx.llm.chat([
        ChatMessage(Role.SYSTEM, "你是通信网络运维专家，输出简洁专业的中文运维报告。"),
        ChatMessage(Role.USER, prompt),
    ])

    # ---- 步骤⑤：会话事实沉淀（长期记忆的写入时机之一）----
    ctx.facts["最近分析设备"] = device or "多设备"
    ctx.facts["最高级别告警"] = top.get("text", "")
    header = f"**[Skill: alarm-analysis v1.0.0]** 已分析 {len(alarms)} 条告警\n\n"
    return header + (resp.content or "(LLM 未返回内容)")
