# AI-Native 智能运维大数据平台（AI-Native Intelligent Operations Big Data Platform）

一个**从零跟着做**的全栈实战学习项目：以通信行业真实运维场景为业务背景，融合 **Kubernetes 云原生、大数据（Kafka / Flink / Spark）、AI Agent（Skill / MCP / Harness）** 三大技术栈，所有实验在本地 Docker 环境即可完整运行，无需真实 LLM API Key。

> 目标学习者：大数据架构师（通信行业方向）
> 项目定位：动手实验 + 面试冲刺双轮驱动。每个模块都有"文档讲透 + 代码能跑 + 面试能答"三重交付。

---

## 1. 项目全景：我们要构建什么

```
                         ┌──────────────────────────────────────────────────┐
                         │                用户 / 运维工程师                    │
                         │        （对话式提问 / Grafana 大盘 / 告警）          │
                         └───────────────▲──────────────────┬───────────────┘
                                         │ 自然语言          │ HTTP/SSE
                         ┌───────────────┴──────────────────▼───────────────┐
                         │           Agent 服务层（agent/harness）            │
                         │   Harness 运行时：上下文管理 · 工具路由 · 权限沙箱     │
                         │   ┌─────────┐  ┌─────────┐  ┌─────────┐          │
                         │   │ Skill:  │  │ Skill:  │  │ Skill:  │          │
                         │   │ 告警分析 │  │ 配置审计 │  │ 流量诊断 │  ...      │
                         │   └────┬────┘  └────┬────┘  └────┬────┘          │
                         │        │   MCP Client（统一接入协议）  │            │
                         └────────┼────────────┼────────────┼───────────────┘
                                  ▼            ▼            ▼
                         ┌────────────┐ ┌──────────┐ ┌──────────────┐
                         │ MCP Server │ │ ES/KPI API│ │ 防火墙控制器   │
                         │ (工具能力)  │ │ (数据检索) │ │ (自动防御)     │
                         └────────────┘ └──────────┘ └──────────────┘
                         ┌──────────────────────────────────────────────────┐
                         │                    数据平台层                       │
                         │  采集: 设备日志/KPI → Kafka(3节点 KRaft)            │
                         │  实时: Flink(CEP 异常检测/Watermark/Checkpoint)     │
                         │  离线: Spark(历史分析/模型训练)                      │
                         │  存储: Elasticsearch(检索) · ClickHouse式OLAP理念   │
                         │  监控: Prometheus + Grafana                        │
                         └──────────────────────────────────────────────────┘
                         ┌──────────────────────────────────────────────────┐
                         │                 云原生底座（K8s 模块）               │
                         │  集群架构 · 核心资源 · 网络/存储/调度 · Operator      │
                         │  Helm/GitOps · RBAC · 可观测性 · HPA/KEDA          │
                         └──────────────────────────────────────────────────┘
```

一句话理解三条主线如何交织：

- **大数据线**回答"数据怎么流动"：设备数据 → Kafka → Flink 实时计算 → 存储 → Spark 离线挖掘；
- **云原生线**回答"服务怎么运行"：一切组件以 K8s 方式部署、伸缩、隔离、观测；
- **Agent 线**回答"智能怎么落地"：Harness 提供运行时舞台，Skill 封装领域能力，MCP 打通外部系统——三者协作完成"人问一句、系统查数、定位根因、自动处置"。

## 2. 知识地图（34 模块 + 4 场景）

### 云原生 · Kubernetes（模块 1-14）

| 阶段 | 模块 | 主题 | 目录 |
|---|---|---|---|
| 基础 | 1 | 集群架构深度剖析（etcd/Raft/kube-proxy） | `k8s/basics/01-cluster-architecture` |
| 基础 | 2 | 核心资源对象（Pod/Deployment/Service/Ingress） | `k8s/basics/02-core-resources` |
| 基础 | 3 | 网络模型（CNI/CoreDNS/NetworkPolicy） | `k8s/basics/03-networking` |
| 基础 | 4 | 存储体系（PV/PVC/StorageClass/CSI） | `k8s/basics/04-storage` |
| 基础 | 5 | 调度机制（预选优选/亲和性/污点） | `k8s/basics/05-scheduling` |
| 基础 | 6 | 命名空间与资源管理（Quota/QoS） | `k8s/basics/06-namespace-resources` |
| 进阶 | 7 | Helm 与 GitOps（ArgoCD/Kustomize） | `k8s/advanced/07-helm-gitops` |
| 进阶 | 8 | RBAC 与安全体系（PSA/OPA/镜像安全） | `k8s/advanced/08-rbac-security` |
| 进阶 | 9 | 可观测性体系（Prometheus Operator/Loki/OTel） | `k8s/advanced/09-observability` |
| 进阶 | 10 | HPA/VPA/KEDA 弹性伸缩 | `k8s/advanced/10-autoscaling` |
| 进阶 | 11 | Operator 开发（CRD/Reconcile/Finalizer） | `k8s/advanced/11-operator` |
| 进阶 | 12 | 集群运维实战（证书/etcd 备份/升级） | `k8s/advanced/12-cluster-ops` |
| 进阶 | 13 | Service Mesh（Istio） | `k8s/advanced/13-service-mesh` |
| 进阶 | 14 | 多集群与混合云 | `k8s/advanced/14-multicluster` |

### 大数据 · Kafka / Flink / Spark（模块 15-25）

| 阶段 | 模块 | 主题 | 目录 |
|---|---|---|---|
| Kafka | 15 | Kafka 架构原理（顺序写/零拷贝/ISR/KRaft） | `bigdata/kafka` |
| Kafka | 16 | 生产者与消费者实战（幂等/事务/再平衡） | `bigdata/kafka` |
| Kafka | 17 | 集群运维与调优（Lag/重分配/故障演练） | `bigdata/kafka` |
| Flink | 18 | 架构与核心概念（时间语义/Watermark/Checkpoint） | `bigdata/flink` |
| Flink | 19 | 流处理实战（DataStream/CEP/Table SQL） | `bigdata/flink` |
| Flink | 20 | 调优与运维（反压/数据倾斜/监控管道） | `bigdata/flink` |
| Spark | 21 | 架构原理（DAG/RDD/Shuffle/Catalyst） | `bigdata/spark` |
| Spark | 22 | SQL 与 DataFrame（窗口函数/多源 Join） | `bigdata/spark` |
| Spark | 23 | 性能调优（倾斜治理/内存/资源规划） | `bigdata/spark` |
| Spark | 24 | Structured Streaming（微批/Exactly-Once） | `bigdata/spark` |
| 综合 | 25 | 端到端数据管道（采集→实时→离线→服务） | `platform/data-pipeline` |

### AI Agent（模块 26-34）

| 阶段 | 模块 | 主题 | 目录 |
|---|---|---|---|
| 基础 | 26 | LLM 基础与 Prompt Engineering | `agent/basics/m26-prompts` |
| 基础 | 27 | LLM API 集成与工程化（Mock Server/适配层） | `agent/basics/m27-llm-adapter` |
| 基础 | 28 | Agent 核心模式（ReAct/Tool Use/Memory） | `agent/basics/m28-agent-core` |
| 核心 | **29** | **Skill vs MCP vs Harness（架构认知核心）** | `agent/harness` `agent/skills` `agent/mcp` |
| 进阶 | 30 | RAG 系统深度构建 | `agent/basics` + `docs/agent` |
| 进阶 | 31 | Tool/Function Calling 深度开发 | `agent/mcp` |
| 进阶 | 32 | 多 Agent 协作 | `agent/advanced/multi-agent` |
| 进阶 | 33 | Agent 可观测性与安全 | `agent/advanced/guardrails` |
| 进阶 | 34 | Agent 部署与运维（vLLM/Operator） | `platform/agent-service` |

### 通信行业融合场景（4 个完整 Lab）

| 场景 | 主题 | 融合点 | 目录 |
|---|---|---|---|
| 一 | 5G 网络切片智能监控 | Kafka→Flink KPI→ES + SLA 分析 Skill + Helm/HPA | `telecom/5g-slice-monitor` |
| 二 | DDoS 攻击检测与自动防御 | Flink CEP + Agent 生成防御规则经 MCP 下发 + Spark 离线 | `telecom/ddos-detection` |
| 三 | 基站智能运维（工业互联） | Modbus/OPC UA + Spark MLlib + 多 Agent 协作 | `telecom/base-station-ops` |
| 四 | NFV 网络服务编排 | K8s CNF 编排 Operator + 服务链 Skill | `telecom/nfv-orchestrator` |

## 3. 学习路线图（三轨并行 → 融合收束）

```
 周次      云原生轨道                大数据轨道                Agent 轨道
─────────────────────────────────────────────────────────────────────────
 第1周    模块1-2 集群架构/资源     模块15-16 Kafka原理/实战   模块26-27 Prompt/API工程化
          （kind 集群上手）          （KRaft 集群上手）          （Mock LLM 上手）
 第2周    模块3-6 网络/存储/调度    模块17 Kafka运维调优       模块28 ReAct/Tool/Memory
 第3周    模块7-9 Helm/安全/观测    模块18-19 Flink概念/CEP    模块29 Skill/MCP/Harness ★
 第4周    模块10-12 伸缩/Operator  模块20 Flink调优运维        场景一：5G切片监控
 第5周    模块13-14 Mesh/多集群     模块21-23 Spark架构/SQL    场景二：DDoS检测防御
 第6周    Operator 综合运用         模块24-25 流处理/大管道     模块30-34 进阶 + 场景三/四
─────────────────────────────────────────────────────────────────────────
 第7周起  面试冲刺：interview/ 四大题库 + 8 道系统设计 + 10 道排障实战 + 覆盖度自查
```

**推荐节奏**：每天 2-3 小时，一个模块 = 文档精读（1h）+ 跑通实验（1h）+ 面试题复盘（0.5h）。
**最低时间预算**：P0 模块（Kafka/Flink/Agent 26-29 + 场景一二）约 40 小时；全部内容约 120 小时。

**贯穿始终的一条主线**（学完应能独立讲出来）：
> 一条 5G 基站 KPI 数据，如何从设备侧产生 → 经 Kafka 削峰 → 被 Flink 按 Watermark 窗口聚合 → CEP 匹配出"切片 SLA 违规"事件 → 写入 ES → Agent 的切片 SLA 分析 Skill 通过 MCP 查询数据并生成结论 → Harness 记录全程审计日志 → 整个平台以 K8s Helm 部署并被 HPA 自动伸缩。

## 4. 快速开始

### 4.1 环境要求

| 项 | 标准模式 | 轻量模式 |
|---|---|---|
| 内存 | 16 GB | 8 GB |
| CPU | 4 核 | 2 核 |
| 磁盘 | 50 GB | 30 GB |
| 软件 | Docker Desktop + docker compose v2 | 同左 |

### 4.2 启动全部基础设施

```bash
cd docker
docker compose -f docker-compose.all.yml up -d        # 标准模式
# 8GB 内存请改用：
# docker compose -f docker-compose.all.yml -f docker-compose.light.yml up -d --scale flink-taskmanager=1
# 需要向量库/日志栈时：追加 --profile milvus / --profile heavy
```

启动后验证（预期输出见各模块 lab 文档）：

```bash
curl -s http://localhost:18000/healthz                  # Mock LLM   → {"status":"ok",...}
curl -s http://localhost:9200                          # ES         → cluster_name "docker-cluster"
docker exec kafka-1 kafka-topics.sh --bootstrap-server localhost:9092 --list   # Kafka 无报错
```

常用入口：Kafka UI http://localhost:8980 · Flink UI http://localhost:8081 · Spark UI http://localhost:8085 · Grafana http://localhost:3000（admin/admin）· Prometheus http://localhost:9090

### 4.3 停止与清理

```bash
docker compose -f docker/docker-compose.all.yml down          # 停止（保留数据卷）
docker compose -f docker/docker-compose.all.yml down -v       # 彻底清空
```

## 5. 目录结构

```
ai-native-ops-platform/
├── README.md                # 本文件：项目总览/知识地图/学习路线
├── PROGRESS.md              # ★ 生成进度与模块完成状态
├── interview-checklist.md   # ★ 面试覆盖度自查表（目标 ≥80%）
├── docs/                    # 知识文档（按方向分目录）
├── k8s/                     # K8s 实战：basics/ advanced/ helm/
├── bigdata/                 # 大数据实战：kafka/ flink/ spark/
├── agent/                   # Agent 实战：basics/ harness/ skills/ mcp/ advanced/
├── telecom/                 # 通信场景融合 Lab（4 个）
├── platform/                # 平台整合：operator/ agent-service/ data-pipeline/ web-ui/
├── docker/                  # ★ 全部 docker-compose 编排（可单独/聚合启动）
├── labs/                    # 跨模块综合实验指南
├── interview/               # 面试题库（K8s/大数据/Agent/系统设计/排障）
└── scripts/                 # 自动化脚本（建 Topic/提交作业/冒烟测试）
```

## 6. 技术选型说明（为何这样选）

| 依赖 | 选型 | 理由（面试高频 + 生产主流 + 学习者友好） |
|---|---|---|
| Kafka | apache/kafka 官方镜像，KRaft 模式 | KRaft 是必考新考点；官方镜像多架构、无第三方封装层 |
| Flink | 1.18 Standalone 会话集群 + Java Maven 工程 | CEP Pattern API 仅 Java 提供；会话集群最易理解作业生命周期 |
| Spark | 3.5 Standalone + PySpark | PySpark 免编译直提交，DataFrame/SQL 是面试与生产主战场 |
| Agent | Python 3.11，框架无关的自研实现为主 | 先"手写 ReAct/Harness"理解本质，再用 LangChain 等框架对比 |
| LLM | Mock LLM Server（OpenAI 协议兼容） | 离线可跑、输出确定性可断言；切真实模型只改 BASE_URL |
| MCP | 自研 JSON-RPC 2.0 实现 + stdio/HTTP 双传输 | 深入协议内部（面试区分度高），另附官方 SDK 用法对照 |
| 存储 | ES 8.12 单节点 / Milvus standalone | 学习配置与生产拓扑等价，差异点在文档中明确标注 |
| 监控 | Prometheus + Grafana | 可观测性事实标准，PromQL 是大数据/云原生面试必考 |

## 7. 常见问题

- **镜像拉取失败（公司网络/代理）**：本项目的 .env 已按"本机缓存优先"选择版本。全新机器可使用国内镜像源前缀拉取后重打标签，命令见 `docker/.env` 文件头注释。
- **8GB 内存跑不动全套**：使用 `docker-compose.light.yml` override（见 4.2），并按模块单独启动 compose 文件，实验完即 `down`。
- **端口冲突**：所有端口已避开 8080/8081/5432 等常见占用（完整映射见 `docker/.env`），冲突时改 .env 即可，代码无需变动。
- **Apple Silicon（M 系列）**：本项目全部镜像均验证过 linux/arm64 可用。

## 8. 配套文档

| 文档 | 用途 |
|---|---|
| [PROGRESS.md](PROGRESS.md) | 各模块生成状态、续写起点 |
| [docs/learning-path.md](docs/learning-path.md) | 42 天逐日学习计划 |
| [interview-checklist.md](interview-checklist.md) | 140+ 高频知识点覆盖度自查 |
| [interview/](interview/) | 四大方向题库 + 系统设计题 + 排障实战题 |
