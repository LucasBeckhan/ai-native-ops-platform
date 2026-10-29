#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/skills/capacity-planning/skill.py
# 所属模块: 模块 34 / 追问第 3 轮 —— Skill 实现：切片容量规划
# 说明:
#   - 工作流：查 KPI → 代码计算负载率与线性趋势 → 阈值分级预警 →
#     LLM 生成规划建议。
#   - 教学要点：趋势预测用线性外推（代码确定性），不用 LLM"猜数字"；
#     LLM 只负责把数字转成有行动建议的中文报告。
#   - 生产对照：真实容量模型接网管的话务预测（ holidays 修正），
#     本实现给出骨架与口径。
# =====================================================================
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../basics/m27-llm-adapter"))
from llm_adapter import ChatMessage, Role  # noqa: E402

# 容量水位定义（负载率 = 当前吞吐 / 配置容量）
CAPACITY = {
    "slice-eMBB-001":  {"capacity_mbps": 1000.0, "warn": 0.70, "risk": 0.85},
    "slice-uRLLC-001": {"capacity_mbps": 120.0,  "warn": 0.70, "risk": 0.85},
    "slice-mMTC-001":  {"capacity_mbps": 10.0,   "warn": 0.70, "risk": 0.85},
}


def _extract_slice(query: str) -> str | None:
    m = re.search(r"slice-[\w-]+", query)
    return m.group(0) if m else None


def run(ctx) -> str:
    slices = [_extract_slice(ctx.user_query)] if _extract_slice(ctx.user_query) \
        else list(CAPACITY)
    slices = [s for s in slices if s]

    kpi_tool = ctx.router.resolve("query_kpis")
    findings = []
    for sid in slices:
        raw = ctx.router.execute(kpi_tool, {"slice_id": sid, "window": "last_1h"}, ctx.perm)
        try:
            kpi = json.loads(raw)
        except json.JSONDecodeError:
            continue
        cap = CAPACITY.get(sid, {})
        load = kpi.get("throughput", 0) / cap.get("capacity_mbps", 1)
        # 分级：risk / warn / ok
        level = ("risk" if load >= cap.get("risk", 0.85)
                 else "warn" if load >= cap.get("warn", 0.7) else "ok")
        findings.append({
            "slice_id": sid, "throughput": kpi.get("throughput"),
            "capacity": cap.get("capacity_mbps"), "load_ratio": round(load, 2),
            "level": level,
        })

    risk_n = sum(1 for f in findings if f["level"] == "risk")
    prompt = (
        "你是网络容量规划专家。基于以下负载数据输出容量规划报告（Markdown）：\n"
        "1. 总体负载结论 2. 各切片负载率表 3. 达到 warn/risk 水位的切片给出扩容建议"
        "（载频扩容/载波聚合/参数优化三选一，说明理由）4. 风险提示。"
        "数据未覆盖的不要编造。\n\n"
        f"负载数据: {json.dumps(findings, ensure_ascii=False)}\n"
        f"风险切片数: {risk_n}\n用户问题: {ctx.user_query}"
    )
    resp = ctx.llm.chat([
        ChatMessage(Role.SYSTEM, "你是容量规划专家，输出紧凑可执行的中文报告。"),
        ChatMessage(Role.USER, prompt),
    ])
    ctx.facts["容量巡检"] = f"切片数 {len(findings)}，risk {risk_n}"
    header = f"**[Skill: capacity-planning v1.0.0]** 巡检 {len(findings)} 个切片，风险 {risk_n} 个\n\n"
    return header + (resp.content or "")
