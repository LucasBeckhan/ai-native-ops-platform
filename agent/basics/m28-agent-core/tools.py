#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/basics/m28-agent-core/tools.py
# 所属模块: 模块 28 —— 工具注册中心与三个运维工具
# 运行方式: 作为库被 react_agent.py 引用；单独运行可自检全部工具
# 说明:
#   - 工具三要素（Function Calling 的根基）：
#     ① JSON Schema 声明（给 LLM 看：有哪些参数、什么类型）；
#     ② 可执行函数（给机器跑：真实实现）；
#     ③ 描述文本（给 LLM 看：什么时候该用我）。
#   - 三个工具模拟真实运维系统的只读能力：查询告警（模拟网管）、
#     查询切片 KPI（模拟性能平台）、运行诊断（模拟诊断脚本）。
#     模块 29 会把同样这三个能力以 MCP Server 形式暴露——工具实现不变，
#     接入协议变，这正是对比实验的基础。
#   - Schema 设计要点：参数尽量少而准（枚举优先于自由字符串）、
#     description 写"何时该用"而非复述函数名。
# =====================================================================
import json
import random
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class Tool:
    """一个可被 Agent 调用的工具 = Schema + 实现 + 元数据。"""
    name: str
    description: str
    parameters: dict          # JSON Schema（OpenAI function 格式）
    func: Callable[..., Any]
    # 权限标记：模块 29 的 Harness 权限沙箱按此字段裁决（read 安全 / write 需授权）
    permission: str = "read"

    def to_openai_schema(self) -> dict:
        """转成 OpenAI tools 声明格式（Mock LLM 与真实厂商通用）。"""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            },
        }


class ToolRegistry:
    """工具注册中心：注册 / 按名查找 / 列出 Schema（喂给 LLM）。"""

    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        if tool.name in self._tools:
            raise ValueError(f"工具重名: {tool.name}")
        self._tools[tool.name] = tool

    def get(self, name: str) -> Tool:
        if name not in self._tools:
            raise KeyError(f"未知工具: {name}（可用: {list(self._tools)}）")
        return self._tools[name]

    def openai_schemas(self) -> list[dict]:
        return [t.to_openai_schema() for t in self._tools.values()]

    def __len__(self) -> int:
        return len(self._tools)


# ---------------------------------------------------------------------------
# 工具实现：模拟网管/性能平台的数据面（固定数据 + 受控随机，可断言）
# ---------------------------------------------------------------------------
_MOCK_ALARMS = [
    {"alarm_id": "A-1001", "device_id": "eNB-1024", "severity": "critical",
     "text": "小区退服", "raised_at": "2026-10-29T08:12:00Z"},
    {"alarm_id": "A-1002", "device_id": "eNB-1024", "severity": "major",
     "text": "光模块接收异常", "raised_at": "2026-10-29T08:12:30Z"},
    {"alarm_id": "A-1007", "device_id": "eNB-1033", "severity": "minor",
     "text": "温度过高预警", "raised_at": "2026-10-29T08:20:00Z"},
]

_MOCK_KPI = {
    "slice-eMBB-001": {"throughput_mbps": 812.4, "latency_ms": 18.2, "packet_loss": 0.001, "users": 15230},
    "slice-uRLLC-001": {"throughput_mbps": 96.1, "latency_ms": 5.1, "packet_loss": 0.0001, "users": 820},
}


def query_alarms(device_id: str = "", severity: str = "", limit: int = 10) -> list[dict]:
    """查询告警列表。过滤条件与网管北向接口对齐。"""
    rows = _MOCK_ALARMS
    if device_id:
        rows = [a for a in rows if a["device_id"] == device_id]
    if severity:
        rows = [a for a in rows if a["severity"] == severity]
    return rows[:limit]


def query_kpis(slice_id: str = "slice-eMBB-001", window: str = "last_1h") -> dict:
    """查询切片 KPI。window 参数模拟真实接口的时间窗语义。"""
    base = dict(_MOCK_KPI.get(slice_id, _MOCK_KPI["slice-eMBB-001"]))
    # 加受控扰动：让两次查询结果略有差异（观察 Agent 的重复查询行为）
    base["throughput_mbps"] = round(base["throughput_mbps"] * (1 + random.uniform(-0.02, 0.02)), 1)
    base["slice_id"] = slice_id
    base["window"] = window
    return base


def run_diagnosis(device_id: str, check_items: list[str] | None = None) -> dict:
    """运行诊断脚本：返回分项检查结果（模拟网管诊断通道）。"""
    items = check_items or ["cpu", "memory", "optical", "links", "config_diff"]
    # 固定剧本：eNB-1024 的光功率异常——配合告警数据讲一个完整故障故事
    results = {i: "normal" for i in items}
    if device_id == "eNB-1024" and "optical" in results:
        results["optical"] = "rx_power=-28.3dBm(阈值-24), 建议检查尾纤/光模块"
    return {"device_id": device_id, "checked": items, "results": results}


def build_default_registry() -> ToolRegistry:
    """组装本项目的默认工具集（模块 28/29 共用）。"""
    reg = ToolRegistry()
    reg.register(Tool(
        name="query_alarms",
        description="查询网络设备告警。当用户问及告警/故障/异常状态时使用。"
                    "device_id 传 eNB-xxxx 形态；severity 可选 critical/major/minor。",
        parameters={
            "type": "object",
            "properties": {
                "device_id": {"type": "string", "description": "设备编号，如 eNB-1024"},
                "severity": {"type": "string", "enum": ["critical", "major", "minor"]},
                "limit": {"type": "integer", "description": "返回条数上限，默认 10"},
            },
            "required": [],
        },
        func=query_alarms,
    ))
    reg.register(Tool(
        name="query_kpis",
        description="查询 5G 网络切片的 KPI（吞吐/时延/丢包/在线用户）。"
                    "当问题涉及切片性能、SLA、流量时使用。",
        parameters={
            "type": "object",
            "properties": {
                "slice_id": {"type": "string", "description": "切片ID，如 slice-eMBB-001"},
                "window": {"type": "string", "description": "时间窗，如 last_1h"},
            },
            "required": ["slice_id"],
        },
        func=query_kpis,
    ))
    reg.register(Tool(
        name="run_diagnosis",
        description="对指定设备运行诊断脚本（CPU/内存/光功率/链路/配置差异）。"
                    "当需要定位设备级根因时使用。",
        parameters={
            "type": "object",
            "properties": {
                "device_id": {"type": "string"},
                "check_items": {"type": "array", "items": {"type": "string"},
                                 "description": "可选，指定检查项"},
            },
            "required": ["device_id"],
        },
        func=run_diagnosis,
    ))
    return reg


if __name__ == "__main__":
    reg = build_default_registry()
    print(f"注册工具数: {len(reg)}")
    for schema in reg.openai_schemas():
        f = schema["function"]
        print(f"  - {f['name']}: {f['description'][:40]}...")
    # 自检：三个工具真实可执行
    print("自检 query_alarms:", json.dumps(query_alarms("eNB-1024"), ensure_ascii=False))
    print("自检 query_kpis:", json.dumps(query_kpis("slice-uRLLC-001"), ensure_ascii=False)[:90], "...")
    print("自检 run_diagnosis:", json.dumps(run_diagnosis("eNB-1024", ["optical"]), ensure_ascii=False))
