#!/usr/bin/env python3
# =====================================================================
# 文件名: agent/harness/src/skill_manager.py
# 所属模块: 模块 29 —— Skill 发现、匹配与执行管理器
# 运行方式: 被 agent_runtime.py 引用；单独运行可自检
# 说明:
#   - Skill 的三大工程问题在本文件给出最小实现：
#     ① 发现（Discover）：扫描 skills/ 目录，读取 skill.json 元数据；
#     ② 路由（Match）：按触发关键词匹配用户意图（教学版子串匹配；
#        生产版见 docs：语义向量匹配 + 置信度阈值 + LLM 兜底裁决）；
#     ③ 执行（Execute）：动态加载 skill.py 的 run()，注入运行时上下文。
#   - 设计原则：Skill 是"自包含能力包"——新增一个 Skill 只需新增目录，
#     运行时零改动（可组合、可发现的落点）。
# =====================================================================
from __future__ import annotations

import importlib.util
import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class SkillMeta:
    name: str
    version: str
    description: str
    triggers: list
    permissions: list
    tools: list
    path: Path

    def render(self) -> str:
        return (f"{self.name} v{self.version} | 触发词: {self.triggers} | "
                f"工具: {self.tools}")


@dataclass
class SkillContext:
    """传给 Skill.run() 的运行时上下文——Skill 与 Harness 的唯一耦合面。"""
    session_id: str
    user_query: str
    router: object            # ToolRouter（取数唯一通道）
    llm: object               # LLMAdapter
    perm: object              # PermissionManager
    facts: dict               # 会话事实（可读写）


class SkillManager:
    def __init__(self, skills_dir: str):
        self.skills_dir = Path(skills_dir)
        self._registry: dict[str, tuple[SkillMeta, object]] = {}
        self._load_all()

    # ---------------- 发现 ----------------
    def _load_all(self) -> None:
        for skill_json in sorted(self.skills_dir.glob("*/skill.json")):
            meta_raw = json.loads(skill_json.read_text(encoding="utf-8"))
            meta = SkillMeta(
                name=meta_raw["name"], version=meta_raw.get("version", "0.0.0"),
                description=meta_raw.get("description", ""),
                triggers=meta_raw.get("triggers", []),
                permissions=meta_raw.get("permissions", ["read"]),
                tools=meta_raw.get("tools", []),
                path=skill_json.parent,
            )
            # 动态加载 skill.py 的 run()
            spec = importlib.util.spec_from_file_location(
                f"skill_{meta.name}", skill_json.parent / "skill.py")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            if not hasattr(module, "run"):
                raise ValueError(f"Skill {meta.name} 缺少 run(ctx) 入口")
            self._registry[meta.name] = (meta, module)

    def list_skills(self) -> list[SkillMeta]:
        return [m for m, _ in self._registry.values()]

    # ---------------- 匹配 ----------------
    def match(self, user_query: str) -> SkillMeta | None:
        """触发词子串匹配（教学版）。命中最先注册的 Skill。
        生产版三阶段：向量相似度初筛 → 规则加分 → LLM 终裁（见 docs）。"""
        best, best_score = None, 0
        for meta, _ in self._registry.items():
            meta_obj = self._registry[meta][0]
            score = sum(1 for t in meta_obj.triggers if t in user_query)
            if score > best_score:
                best, best_score = meta_obj, score
        return best

    # ---------------- 执行 ----------------
    def execute(self, meta: SkillMeta, session_id: str, user_query: str,
                router, llm, perm) -> str:
        _, module = self._registry[meta.name]
        ctx = SkillContext(session_id=session_id, user_query=user_query,
                           router=router, llm=llm, perm=perm, facts={})
        try:
            return module.run(ctx)
        except Exception as e:                  # noqa: BLE001 —— Skill 故障不拖垮会话
            return f"[Skill {meta.name} 执行失败] {type(e).__name__}: {e}"


if __name__ == "__main__":
    sm = SkillManager(os.path.join(os.path.dirname(__file__), "../../skills"))
    print(f"已加载 {len(sm.list_skills())} 个 Skill:")
    for m in sm.list_skills():
        print("  -", m.render())
    q = "eNB-1024 的告警怎么回事，给出处理建议"
    hit = sm.match(q)
    print(f"查询 {q!r} 命中: {hit.name if hit else None}")
