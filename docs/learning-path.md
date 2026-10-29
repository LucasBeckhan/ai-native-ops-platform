# 学习路径规划（42 天逐日计划）

> 配套 [README.md](../README.md) 的知识地图使用。每个模块的详细实验步骤见各模块 `labs/` 目录。
> 每日按 2-3 小时设计；"实验日"请务必动手，只读文档的学习留存率很低。

## 阶段一：基础设施与双轨起步（Day 1-7）

| 天 | 内容 | 产出/验收 |
|---|---|---|
| 1 | 环境搭建：启动 `docker-compose.all.yml`，逐个验证 8 个服务健康 | 冒烟脚本 `scripts/smoke-test.sh` 全绿 |
| 2 | 模块 15 文档：Kafka 架构（Broker/Partition/ISR/KRaft） | 能画出 KRaft 三节点元数据流 |
| 3 | 模块 15 实验：观察 .log/.index 文件、顺序写基准测试 | lab-15 预期输出一致 |
| 4 | 模块 16 实验：Python 生产者/消费者、分区策略 | 消息按 key 落到固定分区 |
| 5 | 模块 16 实验：幂等 + 事务生产者，重复消费对比 | 事务中断后无半消息 |
| 6 | 模块 26 文档+实验：Prompt 模板库、Temperature 对比实验 | 告警分析模板可复用 |
| 7 | 周复盘：整理本周面试题（模块 15/16/26 各 5 道） | 自测答对率 ≥80% |

## 阶段二：流处理与 Agent 核心（Day 8-14）

| 天 | 内容 | 产出/验收 |
|---|---|---|
| 8 | 模块 17 实验：Broker 故障演练、Lag 排查、分区重分配 | 故障恢复时间记录表 |
| 9 | 模块 18 文档：Flink 架构/时间语义/Watermark | 手推 Watermark 传播图 |
| 10 | 模块 18 实验：EventTime 窗口 + 迟到数据处理 | 乱序数据聚合正确 |
| 11 | 模块 19 实验：Checkpoint 与状态恢复 | kill TM 后数据不丢 |
| 12 | 模块 19 实验：CEP 检测"5 分钟内 3 次阈值告警"模式 | 模式命中输出到 ES |
| 13 | 模块 27 实验：LLM 适配层（流式/重试/降级/成本） | 断网自动降级到备用模型 |
| 14 | 模块 28 实验：手写 ReAct 循环 + 工具注册 | Agent 完成一次"查告警→答根因" |

## 阶段三：★ 架构核心周（Day 15-21）

| 天 | 内容 | 产出/验收 |
|---|---|---|
| 15 | 模块 29 文档：Harness/Skill/MCP 三层模型精读 | 能用"OS/App/USB-C"类比讲清三者 |
| 16 | 模块 29 实验：自研 Harness 四大件（上下文/路由/权限/运行时） | 权限拒绝非法工具调用 |
| 17 | 模块 29 实验：实现 3 个通信 Skill | Skill 元数据可被发现路由 |
| 18 | 模块 29 实验：MCP Server（JSON-RPC/stdio/HTTP） | curl 调通 tools/list、tools/call |
| 19 | 模块 29 实验：Harness 集成 MCP Client，端到端联调 | 全链路日志可追溯 |
| 20 | 模块 29 对比实验：直接函数调用 vs MCP 接入 | 填写对比表（成本/隔离/移植） |
| 21 | 场景一开建：数据生成器 + Kafka Topic + Flink KPI 作业 | Grafana 上出现切片 KPI 曲线 |

## 阶段四：K8s 基础 + 融合场景（Day 22-28）

| 天 | 内容 | 产出/验收 |
|---|---|---|
| 22 | K8s 环境：kind 集群 + k8s-tools 容器 | `kubectl get nodes` Ready |
| 23 | 模块 1：集群架构 + etcd 备份恢复实验 | 备份文件可恢复 |
| 24 | 模块 2：Deployment 三种发布策略实战 | 金丝雀流量比例可观测 |
| 25 | 模块 3-4：NetworkPolicy 隔离 + Flink 存储 | 跨 ns 访问被正确拒绝 |
| 26 | 模块 5-6：调度策略 + ResourceQuota | 超配额 Pod 被拒绝 |
| 27 | 场景一收尾：Helm Chart + HPA | 压测时副本自动扩容 |
| 28 | 场景二：DDoS 数据生成 + Flink CEP 攻击检测 | 攻击流量被标记为事件 |

## 阶段五：Spark 与平台整合（Day 29-35）

| 天 | 内容 | 产出/验收 |
|---|---|---|
| 29 | 模块 21：Spark 架构 + RDD 血统实验 | Web UI 看 DAG/Stage 划分 |
| 30 | 模块 22：DataFrame 窗口函数 + 告警关联分析 | 关联分析作业跑通 |
| 31 | 模块 23：数据倾斜治理（加盐/广播 Join 对比） | 倾斜任务耗时对比记录 |
| 32 | 模块 24：Structured Streaming 读 Kafka Exactly-Once | 重启后无重复统计 |
| 33 | 场景二收尾：Agent 经 MCP 下发防火墙规则 + Spark 离线分析 | 攻击 IP 被自动封禁 |
| 34 | 模块 25：端到端大管道（Filebeat→Kafka→Flink→ES→Agent） | 全链路延迟 < 5s |
| 35 | 周复盘 + interview/bigdata-questions.md 自测 | 弱项清单 |

## 阶段六：进阶与面试冲刺（Day 36-42）

| 天 | 内容 | 产出/验收 |
|---|---|---|
| 36 | 模块 30 RAG：知识库构建 + 混合检索 + 评估 | 运维知识库问答可用 |
| 37 | 模块 32-33：多 Agent 协作 + 可观测/安全 | 三 Agent 接力完成故障诊断 |
| 38 | 模块 10-11：HPA/KEDA + Operator 入门 | Kafka 积压触发自动扩容 |
| 39 | 模块 7-9：Helm/GitOps/可观测（进阶速览） | ArgoCD 应用同步成功 |
| 40 | interview/system-design-questions.md：8 道系统设计 | 每道写出架构图+取舍 |
| 41 | interview/troubleshooting-challenges.md：10 道排障 | 每道写出排查命令序列 |
| 42 | interview-checklist.md 全量自查 + 模拟面试 | 覆盖率 ≥80%，弱项闭环 |

## 时间紧张时的最小可行路径（约 40 小时）

只学 P0：模块 15-17（Kafka）→ 18-20（Flink）→ 26-29（Agent 核心）→ 场景一、二。
对应面试收益：覆盖大数据 + Agent 两大方向的最高频考点与最大差异化内容（Skill/MCP/Harness）。

---

## 第二轮深化内容的位置（2026-10-30 补充）

> 以下内容已全部生成并（在可行处）实测，学习时按原有周计划穿插即可：

| 深化内容 | 位置 | 插入周次 |
|---|---|---|
| Ingress 路由 + 金丝雀权重实测（含两处踩坑实录） | k8s/advanced/02-ingress-lab/lab-ingress.md | 第 4 周（模块 2/13 之间） |
| metrics-server 排障四层实录 + HPA 完整生命周期 | k8s/advanced/02-ingress-lab/lab-hpa.md | 第 4 周（模块 10） |
| K8s 进阶层 34 道面试题要点链 | interview/k8s-questions-advanced.md | 随模块 7-14 学习同步刷 |
| RAG 完整实现（混合检索 RRF + 引用式生成 + 命中率评估 5/5） | agent/basics/rag/rag_pipeline.py | 第 6 周（模块 30） |
| Agent 护栏三道（注入检测/脱敏/证据锚定） | agent/advanced/guardrails/guardrails.py | 第 6 周（模块 33） |
| 新增 Skill：容量规划 / 拓扑可视化（Skill 总数 6） | agent/skills/{capacity-planning,topo-visualization}/ | 第 5 周（模块 29 后） |
| Kafka Operator（Go 编译通过 + envtest 骨架） | k8s/advanced/11-operator/kafka-operator/ + README | 第 6 周（模块 11） |
| Agent 面试补充 14 道（RAG/护栏/多 Agent/治理） | interview/agent-questions-supplement.md | 面试冲刺期 |

## 开始之前：环境准备手册

**所有镜像/依赖的获取命令集中在 [environment-prep.md](environment-prep.md)**——
包括 daocloud 离线拉取模板、kind 镜像注入、发行包下载地址、启动顺序与健康检查。
学习前先过一遍该手册的 §1-§3（约 30 分钟），之后每个模块开工前回来查对应条目即可。
