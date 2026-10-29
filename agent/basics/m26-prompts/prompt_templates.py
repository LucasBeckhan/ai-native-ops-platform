#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/basics/m26-prompts/prompt_templates.py
# 所属模块: 模块 26 —— 通信运维场景 Prompt 模板库（五段式结构）
# 运行方式:
#   python3 prompt_templates.py            # 渲染全部模板并调用 Mock LLM 演示
#   python3 -c "from prompt_templates import ALARM_ANALYSIS; print(ALARM_ANALYSIS.render(device_id='eNB-1'))"
# 说明:
#   - 五段式结构：角色设定 → 上下文注入 → 任务定义 → 输出格式约束 → 边界兜底。
#     这五段在面试中可以作为"Prompt 工程化设计"的标准答法。
#   - 模板是"资产"不是"咒语"：版本号 + 渲染函数 + 单测（实验中会验证
#     渲染结果的确定性），改模板要走代码评审。
#   - 所有模板面向 OpenAI Chat 协议（system+user 双消息），与 Mock LLM
#     或真实厂商 API 通用（模块 27 的适配层直接复用）。
# =====================================================================
from dataclasses import dataclass, field


@dataclass
class PromptTemplate:
    """可渲染、可版本化的 Prompt 模板。
    用 {placeholder} 做变量位；render() 对缺失变量直接报错——
    宁可失败也不要渲染出"上下文缺失"的坏 Prompt（静默降级是幻觉源头）。"""

    name: str
    version: str
    system: str
    user: str
    required_vars: set = field(default_factory=set)

    def render(self, **vars: str) -> tuple[str, str]:
        missing = self.required_vars - set(vars)
        if missing:
            raise ValueError(f"模板 {self.name} 缺少变量: {missing}")
        return self.system.format(**vars), self.user.format(**vars)


# ---------------------------------------------------------------------------
# 模板一：告警分析（Zero-shot + 严格 JSON 契约）
# 使用点：采集到告警后第一跳的自动分级与影响面判断
# ---------------------------------------------------------------------------
ALARM_ANALYSIS = PromptTemplate(
    name="alarm-analysis",
    version="1.2.0",
    system=(
        "你是电信网络运维专家，熟悉 5G 基站、核心网网元与传输网络的告警语义。\n"
        "你只依据提供的告警数据做判断，绝不编造数据中不存在的信息。"
    ),
    user=(
        "## 任务\n"
        "分析以下网络告警，输出严重级别与影响面。\n\n"
        "## 告警数据（JSON）\n{alarm_json}\n\n"
        "## 设备上下文\n设备类型：{device_type}；所属区域：{region}；"
        "该设备近 24h 告警数：{recent_alarm_count}\n\n"
        "## 输出格式（严格 JSON，不要输出任何其他文字）\n"
        '{{"severity": "critical|major|minor", "impact": "一句话影响面", '
        '"keywords": ["关键词"], "need_more_info": false}}\n\n'
        "## 分级标准\n"
        "- critical：业务中断（小区退服/链路中断）或影响用户数>100\n"
        "- major：性能劣化（CPU/光功率越限）或影响用户数 10~100\n"
        "- minor：预警类（温度偏高），暂不影响业务\n\n"
        "## 兜底规则\n"
        "若告警数据缺少判断所需关键字段，置 need_more_info=true 并在 impact 说明缺什么。"
    ),
    required_vars={"alarm_json", "device_type", "region", "recent_alarm_count"},
)

# ---------------------------------------------------------------------------
# 模板二：根因定位（CoT + 多假设 + 置信度）
# 使用点：告警聚合后的故障诊断。强制"多假设并行"符合网络故障天然多因的特点。
# ---------------------------------------------------------------------------
ROOT_CAUSE = PromptTemplate(
    name="root-cause-analysis",
    version="1.1.0",
    system=(
        "你是资深网络故障诊断专家。网络故障的根因往往是多假设并存的"
        "（硬件故障/配置错误/容量拥塞/外部施工/软件缺陷），你必须生成多个假设并逐个评估，"
        "不确定时如实说明。禁止编造日志中不存在的事件。"
    ),
    user=(
        "## 故障信息\n"
        "告警列表（JSON）：{alarms_json}\n"
        "近 2 小时变更单：{change_tickets}\n"
        "历史相似案例（知识库检索结果）：{similar_cases}\n\n"
        "## 分析步骤（逐步输出）\n"
        "1. 归纳告警的时空分布特征（同一设备？同一光路？同一时间窗？）\n"
        "2. 列出 2~3 个根因假设\n"
        "3. 逐个假设评估：支持证据 / 反对证据\n"
        "4. 给出最可能根因与置信度（0~1），并说明剩余风险\n\n"
        "## 输出格式（严格 JSON）\n"
        '{{"hypotheses": [{{"cause": "...", "support": "...", "against": "..."}}], '
        '"top_cause": "...", "confidence": 0.0, "next_steps": ["..."]}}\n\n'
        "## 兜底规则\n"
        "证据不足以区分假设时，confidence 必须低于 0.5，且 next_steps 给出"
        "需要补充采集的数据。"
    ),
    required_vars={"alarms_json", "change_tickets", "similar_cases"},
)

# ---------------------------------------------------------------------------
# 模板三：修复建议（带权限边界——Agent 安全的 Prompt 层防线）
# 使用点：根因确认后的处置建议。危险操作强制标注 approval_required，
#         与模块 29 Harness 的权限沙箱形成双保险。
# ---------------------------------------------------------------------------
REMEDIATION = PromptTemplate(
    name="remediation-advisor",
    version="1.0.0",
    system=(
        "你是网络运维自动化顾问。你给出的每个操作步骤都必须评估风险等级，"
        "凡是会影响业务的操作（重启/切换/下发配置）一律标记 approval_required=true，"
        "由人工或权限系统二次确认后才能执行。"
    ),
    user=(
        "## 已确认的根因\n{root_cause}\n\n"
        "## 目标设备\n型号：{device_model}；当前状态：{device_status}；"
        "所在切片：{slice_id}\n\n"
        "## 约束\n"
        "- 操作人权限等级：{permission_level}（L1 只读 / L2 例行操作 / L3 全部）\n"
        "- 业务时段：{business_window}（忙时禁止影响业务的操作）\n\n"
        "## 输出格式（严格 JSON）\n"
        '{{"actions": [{{"step": 1, "command": "...", "risk": "low|medium|high", '
        '"approval_required": true|false}}], '
        '"rollback": "回退方案", "est_impact": "预计影响"}}\n\n'
        "## 兜底规则\n"
        "无法给出安全操作路径时，输出空 actions 并在 est_impact 说明原因，"
        "建议人工介入。"
    ),
    required_vars={
        "root_cause", "device_model", "device_status",
        "slice_id", "permission_level", "business_window",
    },
)

ALL_TEMPLATES = [ALARM_ANALYSIS, ROOT_CAUSE, REMEDIATION]


def demo_with_mock_llm() -> None:
    """用 Mock LLM 走一遍"告警分析"全流程，验证模板可端到端使用。"""
    import json
    import urllib.request

    alarm = {
        "device_id": "eNB-1024", "alarm_code": "ALM-0101",
        "message": "小区退服", "affected_users": 200,
    }
    system, user = ALARM_ANALYSIS.render(
        alarm_json=json.dumps(alarm, ensure_ascii=False),
        device_type="5G gNB", region="华南A市", recent_alarm_count=3,
    )
    body = json.dumps({
        "model": "mock-gpt", "temperature": 0.1,
        "messages": [{"role": "system", "content": system},
                     {"role": "user", "content": user}],
    }).encode()
    req = urllib.request.Request(
        "http://localhost:18000/v1/chat/completions", data=body,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        answer = json.load(resp)["choices"][0]["message"]["content"]
    print("== 告警分析模板 → Mock LLM 响应（前 300 字） ==")
    print(answer[:300])


if __name__ == "__main__":
    # 自检：三个模板都能用最小变量集渲染（缺失变量要报错而不是静默渲染）
    for tpl in ALL_TEMPLATES:
        print(f"模板 {tpl.name} v{tpl.version} 必需变量: {sorted(tpl.required_vars)}")
    try:
        ALARM_ANALYSIS.render(alarm_json="{}")
    except ValueError as e:
        print(f"缺失变量保护生效: {e}")
    demo_with_mock_llm()
