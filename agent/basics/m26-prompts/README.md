# 模块 26：LLM 基础与 Prompt Engineering

> **前置依赖**：无
> **预估学习时间**：5 小时（文档 2h + 实验 2h + 面试题 1h）
> **优先级**：P0

## 学习目标

1. 用一句话讲清 LLM 的生成机制，说清 Token/注意力/位置编码与"上下文窗口"的物理关系；
2. 亲手实现微型 BPE，量化理解中文/JSON 排版对 Token 成本的影响；
3. 掌握 Temperature/TopP 的采样机制与生产选型决策表；
4. 用五段式结构（角色/上下文/任务/格式/兜底）编写三个可版本化的运维 Prompt 模板；
5. 建立 Prompt 资产化管理规范（版本化/变量化/评估集/灰度）。

## 文件清单

| 文件 | 说明 |
|---|---|
| [docs/01-llm-basics.md](docs/01-llm-basics.md) | 知识文档：Transformer 直觉/Tokenization/采样参数/Prompt 方法论 |
| [docs/interview-m26.md](docs/interview-m26.md) | 面试题 6 道（含考察意图/加分回答/误区） |
| [labs/lab-26.md](labs/lab-26.md) | 实验指南：BPE/模板/采样/结构化输出契约 |
| [tokenization_demo.py](tokenization_demo.py) | 从零实现微型 BPE |
| [sampling_demo.py](sampling_demo.py) | Temperature/TopP 采样机制可视化 |
| [prompt_templates.py](prompt_templates.py) | 运维场景 Prompt 模板库（告警分析/根因定位/修复建议） |

## 快速启动

```bash
cd docker && docker compose -f docker-compose.llm.yml up -d   # Mock LLM（宿主机端口 18000）
cd ../agent/basics/m26-prompts
python3 tokenization_demo.py
python3 sampling_demo.py
python3 prompt_templates.py          # 需 Mock LLM 在 18000 端口
```

## 与后续模块的关系

本模块的模板库直接被模块 27（适配层）、28（ReAct）、29（Skill 提示词）复用；
"结构化输出契约"是模块 31（Function Calling 参数解析）的前置知识。
