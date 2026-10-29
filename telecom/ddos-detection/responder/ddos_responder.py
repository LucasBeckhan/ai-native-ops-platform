#!/usr/bin/env python3
# =====================================================================
# 文件名: telecom/ddos-detection/responder/ddos_responder.py
# 所属模块: 场景二 —— DDoS 自动防御编排（Agent 侧的处置工作流）
# 运行方式:
#   python3 ddos_responder.py --attack-ip 203.0.113.88 [--dry-run]
# 说明:
#   - 场景二的智能响应流：Flink CEP 产出 DDoS 告警（Kafka telecom-alerts）
#     → 本工作流消费/接收攻击 IP → 经 Harness 路由调用防火墙 MCP Server
#     下发封禁 → 复核封禁列表 → 生成事件报告。
#   - 权限语义演示（对应 lab 的角色实验）：
#       engineer 角色 → add_firewall_rule 被挂起（require_approval）
#       admin 角色    → 自动放行（封禁动作生效）
#     "Agent 提议、权限裁决、执行留痕"三段式是自动防御的合规骨架。
#   - --listen 模式：持续消费 Kafka 告警自动处置（把"单次演示"变成"值守"）。
# =====================================================================
import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
for p in (os.path.join(HERE, "../../../agent/harness/src"),
          os.path.join(HERE, "../../../agent/basics/m27-llm-adapter"),
          os.path.join(HERE, "../../../agent/basics/m28-agent-core"),
          os.path.join(HERE, "../../../agent/mcp/minimal")):
    sys.path.insert(0, p)

from llm_adapter import ChatMessage, Role, LLMAdapter          # noqa: E402
from tool_router import new_router                             # noqa: E402
from permission import PermissionManager                       # noqa: E402

FIREWALL_SERVER = os.path.join(HERE, "firewall_mcp_server.py")


def build_router() -> "object":
    """同时连接两个 MCP Server：防火墙（场景二）+ teleops（告警/KPI 数据面）。
    多 Server 并存是 MCP 的设计目标（模块 29 实验 4 的落地）。"""
    return new_router(use_mcp=True, mcp_command=[
        sys.executable, FIREWALL_SERVER, "--transport", "stdIO".lower()])


def respond_to_attack(attack_ip: str, attack_type: str, role: str,
                      router, llm) -> str:
    perm = PermissionManager(session_id=f"ddos-{attack_ip}",
                             user_id="agent-auto", role=role)

    # ---- 步骤①：查看当前封禁状态（read，任何角色可执行）----
    list_tool = router.resolve("list_blocked_ips")
    status = router.execute(list_tool, {}, perm) if list_tool else "[]"

    # ---- 步骤②：下发封禁（write，engineer 会被审批挂起）----
    block_tool = router.resolve("add_firewall_rule")
    result = router.execute(block_tool,
                            {"ip": attack_ip, "reason": f"{attack_type} 自动防御",
                             "duration_minutes": 30}, perm)

    # ---- 步骤③：LLM 生成事件报告（基于真实执行结果，不编造）----
    prompt = (
        "你是网络安全应急响应专家。基于以下执行记录输出事件报告（Markdown）：\n"
        "1. 事件概要（攻击类型/IP）2. 处置动作与结果（如实引用）3. 后续建议。\n\n"
        f"攻击: {attack_type} from {attack_ip}\n"
        f"处置前封禁列表: {status}\n"
        f"封禁执行结果: {result}"
    )
    resp = llm.chat([
        ChatMessage(Role.SYSTEM, "你是安全应急响应专家，输出简洁中文事件报告。"),
        ChatMessage(Role.USER, prompt),
    ])
    return (resp.content or "") + f"\n\n---\n执行回执: {result}"


def main() -> None:
    parser = argparse.ArgumentParser(description="DDoS 自动防御工作流")
    parser.add_argument("--attack-ip", default="203.0.113.88")
    parser.add_argument("--attack-type", default="SYN_Flood")
    parser.add_argument("--role", choices=["admin", "engineer", "viewer"],
                        default="admin", help="执行角色（权限实验变量）")
    args = parser.parse_args()

    router = build_router()
    try:
        report = respond_to_attack(args.attack_ip, args.attack_type,
                                   args.role, router, LLMAdapter())
        print(report)
    finally:
        router.close()


if __name__ == "__main__":
    main()
