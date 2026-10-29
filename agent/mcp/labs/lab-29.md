# Lab 29：Skill / MCP / Harness 端到端实战

> **前置**：Mock LLM :18000；模块 28 的工具集已就绪
> **预计耗时**：150 分钟（本模块是全项目最重的实验）
> **实验目标**：① 手写 MCP 协议跑通双传输；② 体验 Harness 四大件的真实约束；③ 跑通三种接入方式的端到端链路；④ 完成对比实验并填写结论。

## 实验一：手写 MCP Server/Client（30 分钟）

```bash
cd agent/mcp/minimal
# stdio 传输自检（握手/发现/调用/资源/提示模板 5 连击）
python3 mcp_client.py
```

**预期输出（实测）**：

```
== stdio 传输 ==
握手成功: teleops-mcp-server 1.0.0 | 能力: ['tools', 'resources', 'prompts']
工具发现: ['query_alarms', 'query_kpis', 'run_diagnosis']
调用结果: {"throughput_mbps": 95.4, ... isError: False
资源读取: 光模块接收异常处置流程：1) 确认收光功率是否低于阈值-24dBm...
提示模板: 请分析设备 eNB-1024 的当前告警：先查询告警列表...

== http 传输 ==
握手成功: teleops-mcp-server | 工具数: 3
调用结果: [{"alarm_id": "A-1001", ... severity critical
```

**协议观察任务**：
- [ ] initialize 应答里的 capabilities 声明了三类能力——Client 由此知道 Server"会什么"；
- [ ] tools/list 的结果在**运行时**发现（Server 加新工具无需改 Client）；
- [ ] tools/call 返回的 isError=false/true 语义（业务失败≠协议失败）。

手动用一行命令体验原始协议（面试时能徒手写出这条最有说服力）：

```bash
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' | \
  python3 mcp_server.py --transport stdio
# 预期输出: {"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05",...}}
```

## 实验二：Harness 权限沙箱（25 分钟）

```bash
cd ../../harness/src
python3 permission.py
```

**预期输出（实测）**：

```
admin     × query_alarms        (read     ) → allow
admin     × set_firewall_rule   (write    ) → allow
admin     × block_ip            (dangerous) → require_approval
engineer  × set_firewall_rule   (write    ) → require_approval
engineer  × block_ip            (dangerous) → deny
viewer    × set_firewall_rule   (write    ) → deny

审批前: require_approval
审批后: allow (role=engineer tool=set_power_threshold level=write [已人工批准])
审计记录数: 3
```

**改造练习**：
1. 给 viewer 角色尝试 approve("block_ip")——观察抛出 PermissionError（dangerous 不可审批，硬边界）；
2. 在 tool_router 的自检里把 set_power_threshold 的 permission 改成 "dangerous"，用 admin 角色调用——观察 require_approval 挂起。

## 实验三：端到端联调 + 对比实验（45 分钟，本模块压轴）

```bash
cd ../../mcp/examples
python3 e2e_demo.py
```

**预期输出（实测摘要）**：

```
模式 A-direct: 本地函数直调
Q: eNB-1024 的告警怎么回事，给出处理建议
  [Skill=alarm-analysis 耗时=4ms]
  A: **[Skill: alarm-analysis v1.0.0]** 已分析 2 条告警  ### 分析报告...
审计轨迹（ToolRouter 记录）: ['query_alarms', 'run_diagnosis', 'query_kpis', 'run_diagnosis']

模式 B-mcp: MCP Server 接入
（同样的三个问题、同样的 Skill 命中、同样的审计轨迹）
对比实验结论（实测数据）
| 维度 | A 直接函数调用 | B MCP Server 接入 |
| 三问题总耗时 | ~10 ms | ~5 ms（本机 stdio） |
| 进程隔离 | 无 | 有 |
...
```

**验收清单**：
- [ ] 两个模式下三个 Skill 全部正确命中（告警/流量/配置审计）；
- [ ] 审计轨迹完全一致（Harness 抽象屏蔽了接入差异）；
- [ ] traffic-diagnosis 经 MCP resources 读取了 SLA 阈值（skill.py 的 _sla_from_mcp）；
- [ ] 对比表填写完成，能用一句话回答"为什么需要 MCP"。

## 实验四：做一个新 MCP Server（50 分钟，综合能力检验）

目标：实现 `firewall-mcp-server`（场景二将复用）：
1. 复制 `minimal/mcp_server.py` 结构，暴露两个工具：
   - `add_firewall_rule(ip, direction)` —— write 级别（返回模拟的成功确认）
   - `list_blocked_ips()` —— read 级别
2. 在 Harness 中同时连接两个 Server（router._connect_mcp 支持多实例，或改造为列表）；
3. 提问"把攻击 IP 1.2.3.4 加入封禁"，观察：
   - engineer 角色 → require_approval（写操作被挂起）；
   - admin 角色 → allow（工具执行，审计留痕）。

**验收**：两个 Server 的工具在同一会话内可混合调用（MCP 多 Server 并存的实证）。
