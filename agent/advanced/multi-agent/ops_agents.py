#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/advanced/multi-agent/ops_agents.py
# 所属模块: 模块 32 / 场景三 —— 多 Agent 协作（监控→诊断→工单接力）
# 运行方式:
#   python3 ops_agents.py   （需 Mock LLM :18000；MCP Server 由脚本拉起）
# 说明:
#   - 模块 32 的核心实验："网络故障诊断多 Agent 系统"：
#       MonitorAgent  → 查询告警，发现异常则生成"事件单"
#       DiagnoseAgent → 接事件单，运行诊断工具，产出根因假设
#       TicketAgent   → 接根因结论，生成工单（写操作→权限矩阵裁决）
#   - 三条设计原则（文档 32 节 + 模块 29 Q11 的落地）：
#     ① Agent 间传"结构化事件单"而非共享原始上下文（信息按需最小化）
#     ② 每个 Agent 独立角色权限（监控只读/工单写操作需审批）
#     ③ Harness 统一裁决所有工具调用（审计不断链）
#   - 与单 Agent ReAct 的差异：单 Agent 自己规划全流程（贵、路径不定）；
#     多 Agent 是"固定接力 + 每棒内自治"（可控、可审计、可独立重跑）。
# =====================================================================
import json
import os
import sys
import time
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
for p in (os.path.join(HERE, "../../harness/src"),
          os.path.join(HERE, "../../basics/m27-llm-adapter"),
          os.path.join(HERE, "../../basics/m28-agent-core"),
          os.path.join(HERE, "../../mcp/minimal")):
    sys.path.insert(0, p)

from llm_adapter import ChatMessage, Role, LLMAdapter       # noqa: E402
from tool_router import new_router                          # noqa: E402
from permission import PermissionManager                    # noqa: E402


class AgentBase:
    """接力棒上的一个 Agent：角色提示 + 有限工具 + 独立权限。"""

    name = "base"
    system_prompt = ""

    def __init__(self, router, llm):
        self.router = router
        self.llm = llm

    def ask(self, task: str) -> str:
        """单轮 LLM 判断（Agent 间协作的'思考'只做一件事，不做长循环）。"""
        resp = self.llm.chat([
            ChatMessage(Role.SYSTEM, self.system_prompt),
            ChatMessage(Role.USER, task),
        ])
        return resp.content or ""


class MonitorAgent(AgentBase):
    """第一棒：监控。只读权限，发现异常 → 生成结构化事件单。"""

    name = "monitor"
    system_prompt = ("你是网络监控值班 Agent。基于告警数据判断是否存在需要处置的异常。"
                     "输出 JSON：{\"event\": true/false, \"device_id\": \"...\", "
                     "\"summary\": \"一句话\", \"severity\": \"...\"}，不要输出其他文字。")

    def scan(self, device_id: str | None, perm: PermissionManager) -> dict | None:
        tool = self.router.resolve("query_alarms")
        raw = self.router.execute(tool, {"device_id": device_id} if device_id else {}, perm)
        try:
            alarms = json.loads(raw)
        except json.JSONDecodeError:
            return None
        critical = [a for a in alarms if a.get("severity") in ("critical", "major")]
        if not critical:
            return None
        event = {
            "event_id": f"EVT-{uuid.uuid4().hex[:8]}",
            "device_id": critical[0]["device_id"],
            "severity": critical[0]["severity"],
            "summary": f"{len(critical)} 条告警：{critical[0]['text']}",
            "alarms": critical,
            "ts": time.time(),
        }
        return event


class DiagnoseAgent(AgentBase):
    """第二棒：诊断。接事件单 → 运行诊断工具 → 根因假设。"""

    name = "diagnose"
    system_prompt = ("你是网络诊断 Agent。基于事件单与诊断结果给出根因假设与置信度。"
                     "输出 JSON：{\"root_cause\": \"...\", \"confidence\": 0.0, "
                     "\"action\": \"建议动作\"}。")

    def diagnose(self, event: dict, perm: PermissionManager) -> dict:
        diag_tool = self.router.resolve("run_diagnosis")
        diag = self.router.execute(diag_tool, {"device_id": event["device_id"]}, perm)
        task = (f"事件单: {json.dumps(event, ensure_ascii=False)}\n"
                f"诊断结果: {diag}")
        try:
            return json.loads(self.ask(task))
        except json.JSONDecodeError:
            # 兜底路径（生产要点）：LLM 判断失败时用代码规则从诊断数据直接推导——
            # 结构化数据的确定性优先，LLM 只是"锦上添花"的叙述层
            fallback = {"root_cause": "待人工确认", "confidence": 0.0,
                        "action": "人工介入", "diag": diag[:150]}
            try:
                diag_data = json.loads(diag)
                abnormal = {k: v for k, v in diag_data.get("results", {}).items()
                            if v != "normal"}
                if abnormal:
                    key, val = next(iter(abnormal.items()))
                    fallback = {"root_cause": f"{key}: {val}", "confidence": 0.9,
                                "action": "按诊断建议处置", "diag": diag[:150]}
            except json.JSONDecodeError:
                pass
            return fallback


class TicketAgent(AgentBase):
    """第三棒：工单。写操作——权限矩阵会裁决（viewer/engineer 差异可实验）。"""

    name = "ticket"
    system_prompt = ("你是工单 Agent。根据根因结论生成工单描述（中文，50 字内）。"
                     "直接输出工单正文。")

    def create_ticket(self, event: dict, diagnosis: dict, perm: PermissionManager,
                      llm_report: str) -> dict:
        # 写操作示例：经 MCP 在"工单系统"登记（此处用防火墙列表的只读工具
        # 演示跨 Server 组合；真实工单工具在场景四的 NFV Server 中扩展）
        tool = self.router.resolve("list_blocked_ips")
        ops_status = self.router.execute(tool, {}, perm) if tool else "n/a"
        return {
            "ticket_id": f"TKT-{uuid.uuid4().hex[:6]}",
            "device_id": event["device_id"],
            "root_cause": diagnosis.get("root_cause"),
            "confidence": diagnosis.get("confidence"),
            "report": llm_report[:200],
            "related_ops": "firewall-mcp-server 可达",
        }


def main() -> None:
    llm = LLMAdapter()
    router = new_router(use_mcp=True, mcp_command=[
        sys.executable, os.path.join(HERE, "../../mcp/minimal/mcp_server.py")])
    try:
        # 各 Agent 的权限矩阵：监控只读 / 诊断只读 / 工单拥有 write 权限实验位
        monitor_perm = PermissionManager("chain", "monitor-bot", role="viewer")
        diag_perm = PermissionManager("chain", "diagnose-bot", role="engineer")
        ticket_perm = PermissionManager("chain", "ticket-bot", role="engineer")

        monitor, diagnose, ticket = (MonitorAgent(router, llm),
                                     DiagnoseAgent(router, llm),
                                     TicketAgent(router, llm))

        # ---- 接力执行 ----
        print("[1/3] MonitorAgent 巡检...")
        event = monitor.scan("eNB-1024", monitor_perm)
        if not event:
            print("  无需处置的异常，链路结束")
            return
        print(f"  事件单: {event['event_id']} ({event['summary']})")

        print("[2/3] DiagnoseAgent 诊断...")
        diagnosis = diagnose.diagnose(event, diag_perm)
        print(f"  根因: {diagnosis.get('root_cause')} (置信度 {diagnosis.get('confidence')})")

        print("[3/3] TicketAgent 建单...")
        t = ticket.create_ticket(event, diagnosis, ticket_perm,
                                 diagnosis.get("action", ""))
        print(f"  工单: {t['ticket_id']} → {t['root_cause']}")

        print("\n审计轨迹（Router 统一留痕）:")
        for a in router.audit:
            print(f"  - {a['tool']} args={a['args']} {a['ms']}ms")
    finally:
        router.close()


if __name__ == "__main__":
    main()
