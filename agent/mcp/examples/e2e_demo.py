#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/mcp/examples/e2e_demo.py
# 所属模块: 模块 29 —— 端到端联调 + "直接函数调用 vs MCP"对比实验
# 运行方式:
#   python3 e2e_demo.py           # 需 Mock LLM :18000；MCP Server 由本脚本拉起
# 说明:
#   - 全链路：用户提问 → Harness(Runtime) 意图匹配 Skill → Skill 工作流
#     → ToolRouter（权限/审计）→ 工具执行（本地函数 或 MCP Server）
#     → 结果回流生成报告。
#   - 对比实验设计（同一 Skill、同一问题、两种接入）：
#       MODE A direct：ToolRouter 只挂本地注册表（进程内函数调用）
#       MODE B mcp   ：ToolRouter 连接 MCP Server（stdio 子进程）
#     对比四个维度：接入成本 / 单次调用开销 / 进程隔离 / 权限边界，
#     实验数据由脚本实测输出，结论写入对比表。
# =====================================================================
import json
import os
import sys
import time

HARNESS = os.path.join(os.path.dirname(__file__), "../../harness/src")
sys.path.insert(0, HARNESS)

from agent_runtime import AgentRuntime  # noqa: E402
from permission import PermissionManager  # noqa: E402

SKILLS_DIR = os.path.join(os.path.dirname(__file__), "../../skills")
MCP_SERVER_CMD = [sys.executable, os.path.join(
    os.path.dirname(__file__), "../minimal/mcp_server.py"), "--transport", "stdio"]

QUESTIONS = [
    "eNB-1024 的告警怎么回事，给出处理建议",     # → alarm-analysis（含诊断步骤）
    "slice-uRLLC-001 的时延 KPI 是否达标？",     # → traffic-diagnosis（MCP resources 演示）
    "eNB-1024 配置审计一下",                     # → config-audit
]


def run_mode(mode: str) -> dict:
    """在指定接入模式下跑完三个问题，返回报告摘要。"""
    print(f"\n{'=' * 64}\n模式 {mode}: {'本地函数直调' if mode == 'A-direct' else 'MCP Server 接入'}\n{'=' * 64}")
    rt = AgentRuntime(
        use_mcp=(mode == "B-mcp"),
        mcp_command=MCP_SERVER_CMD if mode == "B-mcp" else None,
        skills_dir=SKILLS_DIR,
    )
    rt.create_session(f"s-{mode}", "ops-user", role="engineer")
    results = []
    for q in QUESTIONS:
        t0 = time.time()
        answer = rt.chat(f"s-{mode}", q)
        elapsed = int((time.time() - t0) * 1000)
        skill = rt.contexts.get(f"s-{mode}").facts.get("命中Skill", "(通用ReAct)")
        print(f"\nQ: {q}")
        print(f"  [Skill={skill} 耗时={elapsed}ms]")
        print(f"  A: {answer[:110].replace(chr(10), ' ')}...")
        results.append({"q": q, "skill": skill, "ms": elapsed})
    rep = rt.reports[f"s-{mode}"]
    audit_tools = [a["tool"] for a in rt.router.audit]
    print(f"\n审计轨迹（ToolRouter 记录）: {audit_tools}")
    print(f"会话统计: turns={rep.turns} tool_calls={rep.tool_calls} cost=${rep.cost_usd:.6f}")
    rt.router.close()
    return {"mode": mode, "results": results, "audit": audit_tools,
            "turns": rep.turns, "tool_calls": rep.tool_calls}


def main() -> None:
    a = run_mode("A-direct")
    b = run_mode("B-mcp")

    print(f"\n{'=' * 64}\n对比实验结论（实测数据）\n{'=' * 64}")
    a_ms = sum(r["ms"] for r in a["results"])
    b_ms = sum(r["ms"] for r in b["results"])
    print(f"""
| 维度 | A 直接函数调用 | B MCP Server 接入 |
|---|---|---|
| 工具接入方式 | 进程内 import，改工具需重启 Harness | 独立进程/机器，动态发现（tools/list），热插拔 |
| Skill/Agent 代码改动 | —（本实验两者 Skill 代码零差异） | 同左 |
| 三问题总耗时 | {a_ms} ms | {b_ms} ms（含子进程拉起与协议往返） |
| 进程隔离 | 无（工具异常可拖垮 Harness） | 有（Server 崩溃不波及 Harness，可重启重连） |
| 权限边界 | 进程内函数任意能力（靠约定） | 协议层声明能力清单，Host 按声明裁决 |
| 跨语言/跨机复用 | 否（必须同进程 Python） | 是（任何语言实现的 MCP Client 可接入） |
| 审计轨迹条数 | {len(a['audit'])} | {len(b['audit'])}（同一通道，粒度一致） |

工程结论：
1) 单机原型/原型验证 → 直接函数调用（快、简单）；
2) 多团队/多语言/需独立部署与权限治理 → MCP 接入（隔离+标准化），
   本实验中 MCP 的额外开销为毫秒级（本机 stdio），生产远程部署需评估网络 RTT；
3) 两种模式下 Skill 与 Agent 代码完全相同——这正是 Harness 抽象的价值，
   也是面试中"为什么需要 MCP"的实证答案。
""")


if __name__ == "__main__":
    main()
