#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/skills/config-audit/skill.py
# 所属模块: 模块 29 —— Skill 实现：基站配置审计
# 说明:
#   - 工作流：提取设备ID → 经 Router 运行 config_diff 诊断 →
#     确定性规则比对合规基线（LLM 不参与规则判定，只解读结果）→ 报告。
#   - 设计点：**规则判定用代码不用 LLM**——合规审计要求 100% 可复现、
#     可审计，LLM 的概率性输出在这里是负资产。这是"Skill 封装领域知识"
#     的典型体现：领域专家的基线规则沉淀为代码。
# =====================================================================
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../basics/m27-llm-adapter"))
from llm_adapter import ChatMessage, Role  # noqa: E402

# 合规基线（生产中来自配置管理库/等保要求，此处为演示子集）
BASELINE_RULES = {
    "config_diff": "设备配置与基线必须一致，不允许未评审的差异",
    "cpu": "CPU 利用率应低于 80%",
}


def _extract_device(query: str) -> str | None:
    m = re.search(r"eNB-\d{4}", query)
    return m.group(0) if m else None


def run(ctx) -> str:
    device = _extract_device(ctx.user_query)
    if not device:
        return "[config-audit] 请在问题中指明设备编号（如 eNB-1024）"

    diag_tool = ctx.router.resolve("run_diagnosis")
    if not diag_tool:
        return "[config-audit] 环境中缺少 run_diagnosis 工具"
    raw = ctx.router.execute(
        diag_tool, {"device_id": device, "check_items": ["config_diff", "cpu"]}, ctx.perm)
    try:
        diagnosis = json.loads(raw)
    except json.JSONDecodeError:
        return f"[config-audit] 诊断结果不可解析: {raw[:100]}"

    # ---- 确定性规则比对（代码判定，不经过 LLM）----
    violations = []
    for item, result in diagnosis.get("results", {}).items():
        if item == "config_diff" and result != "normal":
            violations.append({"rule": BASELINE_RULES[item], "finding": str(result)})
        if item == "cpu" and result != "normal":
            violations.append({"rule": BASELINE_RULES[item], "finding": str(result)})

    verdict = "合规" if not violations else f"发现 {len(violations)} 项不合规"
    if violations:
        prompt = (
            "你是网络配置审计专家。基于以下违规事实输出整改清单（Markdown 表格："
            "项/违规说明/建议动作/风险等级）。只依据给定事实，不编造。\n"
            f"设备: {device}\n违规列表: {json.dumps(violations, ensure_ascii=False)}"
        )
        resp = ctx.llm.chat([
            ChatMessage(Role.SYSTEM, "你是配置审计专家，输出紧凑的整改表格。"),
            ChatMessage(Role.USER, prompt),
        ])
        body = resp.content or ""
    else:
        body = "所有检查项与基线一致，无需整改。"

    ctx.facts["最近审计设备"] = device
    ctx.facts["审计结论"] = verdict
    header = f"**[Skill: config-audit v1.0.0]** 设备 {device} 审计结论: {verdict}\n\n"
    return header + body
