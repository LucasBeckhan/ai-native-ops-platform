# Lab 26：Tokenization 与 Prompt 工程化实验

> **前置**：Mock LLM 已启动（`docker compose -f docker/docker-compose.llm.yml up -d` 或本机 uvicorn）
> **预计耗时**：60 分钟
> **实验目标**：① 亲手实现 BPE 理解 Token 成本结构；② 掌握模板五段式与缺失变量保护；③ 建立"Prompt 资产化管理"的肌肉记忆。

## 实验一：微型 BPE 训练与领域分词（20 分钟）

```bash
cd agent/basics/m26-prompts
python3 tokenization_demo.py
```

**预期输出**（完整版）：

```
训练完成：学习到 30 条合并规则，前 8 条：
   'e' + '_' -> 'e_'
   't' + 'a' -> 'ta'
   'a' + 'l' -> 'al'
   'al' + 'a' -> 'ala'
   'ala' + 'r' -> 'alar'
   'alar' + 'm' -> 'alarm'
   'alarm' + '_' -> 'alarm_'
   'b' + 'a' -> 'ba'

== 实验一：领域词的切分对比（语料内 vs 语料外） ==
  'alarm base_station outage'            -> 3 tokens: ['alarm_', 'base_station_', 'outage_']
  'alarm firewall log'                   -> 13 tokens: ['alarm_', 'f', 'i', 'r', 'e', 'w', 'al', 'l', '_', 'l', 'o', 'g', '_']
...
```

**观察任务**：
- [ ] `alarm`（语料高频词）被合并为 1 个 Token，`firewall`（语料外）被切碎成 6 个——这就是**领域专用模型/微调语料的价值**：分词器与模型都"认识"领域词汇；
- [ ] 中文实验：24 字 ≈ 25 tokens（中文按 UTF-8 字节级切分，成本高）；
- [ ] JSON 紧凑格式省约 19% tokens——把这条规则写进你的模板规范。

## 实验二：五段式模板与缺失变量保护（15 分钟）

```bash
python3 prompt_templates.py
```

**预期输出**：

```
模板 alarm-analysis v1.2.0 必需变量: ['alarm_json', 'device_type', 'recent_alarm_count', 'region']
模板 root-cause-analysis v1.1.0 必需变量: ['alarms_json', 'change_tickets', 'similar_cases']
模板 remediation-advisor v1.0.0 必需变量: ['business_window', 'device_model', 'device_status', 'permission_level', 'root_cause', 'slice_id']
缺失变量保护生效: 模板 alarm-analysis 缺少变量: {'device_type', 'region', 'recent_alarm_count'}
== 告警分析模板 → Mock LLM 响应（前 300 字） ==
...
```

**改造练习**（学以致用）：
1. 给 `ALARM_ANALYSIS` 增加 `alarm_time` 变量，并在 user 段要求"结合发生时段判断"；
2. 故意不传 `region` 运行——确认抛出 ValueError（**Prompt 的失败必须显式**，静默降级是幻觉温床）；
3. 把版本号升为 1.3.0，模拟一次"模板变更走评审"的流程。

## 实验三：Temperature 参数实验（10 分钟）

```bash
python3 sampling_demo.py
```

观察三种温度下输出分布的差异（脚本用本地概率分布模拟采样过程，结果确定可复现），并对照文档 1.2 决策表回答：
- [ ] 告警分级任务用 T=0 的理由？
- [ ] 为什么"修复建议"允许 T=0.6？

## 实验四：结构化输出契约（15 分钟）

用模板 + Mock LLM 完成"渲染 → 请求 → JSON 解析"闭环，并故意制造一次解析失败：

```python
# 思考题脚本骨架（自己补全并运行）
import json, urllib.request
from prompt_templates import ALARM_ANALYSIS
system, user = ALARM_ANALYSIS.render(
    alarm_json='{"device_id":"eNB-1","message":"小区退服"}',
    device_type="gNB", region="华南", recent_alarm_count="2")
body = json.dumps({"model": "mock-gpt", "messages": [
    {"role": "system", "content": system}, {"role": "user", "content": user}]}).encode()
req = urllib.request.Request("http://localhost:18000/v1/chat/completions", data=body,
                             headers={"Content-Type": "application/json"})
resp = json.load(urllib.request.urlopen(req, timeout=10))
content = resp["choices"][0]["message"]["content"]
# 思考：直接 json.loads(content) 会发生什么？生产中如何设计"解析失败重试"？
```

**参考答案方向**：① Prompt 中已约束"严格 JSON"，但真实模型仍可能带 markdown 代码块包裹——工程上要做"剥离代码围栏 + 重试一次 + 降级人工"三层；② 模块 27 的适配层会实现这个 JSON 提取器。
