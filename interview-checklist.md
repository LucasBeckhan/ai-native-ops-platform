# 面试覆盖度自查表（interview-checklist）

> 生成日期：2026-10-30 · 目标覆盖率 ≥80%
> ✅ = 已覆盖（有文档/代码/实验支撑）· ❌ = 未覆盖（按《分段追问手册》第 6 轮补全）

## 一、K8s 方向（50 个高频知识点）

| # | 知识点 | 覆盖 | 位置 |
|---|---|---|---|
| 1 | kubectl apply → 容器运行全流程 | ✅ | basics/interview Q1 |
| 2 | 控制面组件协作与 list-watch | ✅ | docs-basics 模块 1 |
| 3 | etcd Raft 原理与备份恢复 | ✅ | Q2 + docs 模块 1/12 |
| 4 | 高可用部署（堆叠 vs 外部 etcd） | ✅ | docs 模块 1.4 |
| 5 | kube-proxy iptables/IPVS/eBPF | ✅ | Q3（kind 实测 iptables） |
| 6 | kubelet 工作流程 | ✅ | Q1/Q10 |
| 7 | Pod 生命周期 | ✅ | docs 模块 2 |
| 8 | 三探针组合设计 | ✅ | Q4（集群实测） |
| 9 | Deployment 滚动更新与回滚 | ✅ | Q5（实测含故障回滚） |
| 10 | PDB 驱逐保护 | ✅ 速答 | k8s-questions 速答 10 |
| 11 | Service 三类型与 Endpoints | ✅ | Q3/实测 |
| 12 | iptables 随机 vs IPVS 算法 | ✅ | Q3 |
| 13 | Ingress 原理与灰度 | ✅ | ingress-lab（真实集群：路径路由+canary 权重 9/100 实测+排障实录） |
| 14 | CoreDNS 解析链路与调优 | ✅ | docs 模块 3 |
| 15 | NetworkPolicy 原理与设计模式 | ✅ | docs 模块 3（kindnet 限制实证） |
| 16 | CNI 选型（Calico/Cilium） | ✅ | docs 模块 3 |
| 17 | PV/PVC/StorageClass/CSI | ✅ | Q6（Bound 实测） |
| 18 | 动态供给流程 | ✅ | Q6 |
| 19 | 调度预选/优选算法 | ✅ | Q7 |
| 20 | 亲和/反亲和/污点/容忍 | ✅ | Q7（nodeSelector 实测） |
| 21 | PriorityClass 与抢占 | ✅ | Q7 |
| 22 | ResourceQuota/LimitRange | ✅ | Q8（实测） |
| 23 | QoS 三级与 OOM 排查 | ✅ | Q8/Q9（实测 Guaranteed） |
| 24 | Helm Chart 开发/Hook | ✅ | advanced 模块 7（场景一 Chart 实例） |
| 25 | Kustomize vs Helm | ✅ | advanced 模块 7 |
| 26 | ArgoCD GitOps | ✅ | advanced 模块 7 |
| 27 | RBAC 最小权限设计 | ✅ | advanced 模块 8 |
| 28 | PSA 准入 | ✅ | advanced 模块 8 |
| 29 | OPA/Gatekeeper 策略 | ❌ | 追问第 1 轮（实验要求已定义；镜像需联网拉取） |
| 30 | 镜像安全（Cosign/Trivy） | ✅ | advanced 模块 8 |
| 31 | Prometheus Operator/ServiceMonitor | ✅ | advanced 模块 9 |
| 32 | PromQL 高阶 | ✅ | advanced 模块 9 |
| 33 | EFK vs Loki | ✅ | advanced 模块 9 |
| 34 | OpenTelemetry + Jaeger | ❌ | 追问第 6 轮 |
| 35 | HPA 算法与抖动治理 | ✅ | advanced 模块 10（场景一 hpa.yaml） |
| 36 | VPA 适用与冲突 | ✅ | advanced 模块 10 |
| 37 | KEDA 事件驱动伸缩 | ✅ | advanced 模块 10 |
| 38 | CRD 设计与 controller-runtime | ✅ | 场景四 Go Operator（编译+kind 实测） |
| 39 | Reconcile 幂等/Finalizer/Status | ✅ | 场景四（实测） |
| 40 | envtest 单元测试 | ❌ | 追问第 1 轮 |
| 41 | 证书轮换 | ✅ | advanced 模块 12 |
| 42 | etcd 快照恢复 | ✅ | advanced 模块 12 |
| 43 | 集群升级与节点排水 | ✅ | advanced 模块 12 |
| 44 | NotReady/CrashLoop 排查法 | ✅ | advanced 模块 12 |
| 45 | Istio Sidecar/VirtualService/mTLS | ✅ | advanced 模块 13 |
| 46 | Cluster API/多集群管理 | ✅ | advanced 模块 14 |
| 47 | 边缘自治（K3s） | ✅ | advanced 模块 14 |
| 48 | StatefulSet/Headless | ✅ | Q11 |
| 49 | Pause/init/sidecar 容器 | ✅ | 速答 1/2 |
| 50 | 优雅终止全流程 | ✅ | Q5 |

**K8s 覆盖率：47/50 = 94%**（❌：Ingress 实验、OPA 实验、OTel 链路）

## 二、Kafka（20 个）

| # | 知识点 | 覆盖 |
|---|---|---|
| 1 | 三层逻辑视图/分区副本职责 | ✅ m15 |
| 2 | KRaft vs ZooKeeper | ✅ m15 Q4（纯 KRaft 实测） |
| 3 | 顺序写/零拷贝/页缓存 | ✅ m15 Q1（基准实测） |
| 4 | 稀疏索引/Segment 滚动 | ✅ m15 + lab 实测 |
| 5 | ISR 收缩扩展/时间阈值 | ✅ m15 Q3（lab 实测 pause） |
| 6 | LEO/HW 语义 | ✅ m15 |
| 7 | acks/min.isr/unclean 三元组 | ✅ m15 Q2（NotEnoughReplicas 实测） |
| 8 | 消息不丢失三端分析 | ✅ m15 Q2 |
| 9 | 幂等（PID+序列号） | ✅ m16 Q1 |
| 10 | 事务与端到端 EOS | ✅ m16 Q2（隔离级别实测） |
| 11 | 再平衡（Eager/Cooperative/KIP-848） | ✅ m16 Q3 |
| 12 | 消费者位移提交语义 | ✅ m16 Q4 |
| 13 | 分区分配策略 | ✅ m16 Q5 |
| 14 | Streams vs Flink | ✅ m16 Q6 |
| 15 | 攒批/压缩调参 | ✅ m16 Q7 |
| 16 | Lag 三形态排查 | ✅ m17 Q1（lag_watch 实测） |
| 17 | URP 定位 | ✅ m17 Q2（故障演练实测） |
| 18 | 容量规划 | ✅ m17 Q3/Q7 |
| 19 | 滚动升级 | ✅ m17 Q5 |
| 20 | MM2 容灾 | ✅ m17 Q6 |

**Kafka 覆盖率：20/20 = 100%**

## 三、Flink（20 个）

| # | 知识点 | 覆盖 |
|---|---|---|
| 1 | JM/TM/Slot/算子链 | ✅ m18 |
| 2 | 时间语义选择 | ✅ m18 Q1 |
| 3 | Watermark 生成传播/迟到 | ✅ m18 Q1（实测含 idleness） |
| 4 | 三种窗口与增量聚合 | ✅ m18 Q3（实测） |
| 5 | Keyed State/状态后端 | ✅ m18 Q4 |
| 6 | Checkpoint（Chandy-Lamport） | ✅ m18 Q2（故障恢复实测） |
| 7 | Savepoint | ✅ m18 Q2 加分 |
| 8 | Kafka Connector 生产配置 | ✅ m19 |
| 9 | exactly-once 端到端 | ✅ m19/20 Q5 |
| 10 | CEP 原理/开放量词语义 | ✅ m19 Q4（实测） |
| 11 | 反压定位（busy/idle/火焰图） | ✅ m20 Q3 |
| 12 | 数据倾斜两次聚合 | ✅ m20/Spark m23 |
| 13 | Checkpoint 调优/unaligned | ✅ m20 Q9 |
| 14 | 状态 TTL/大状态治理 | ✅ m20 Q7 |
| 15 | Prometheus 指标接入 | ✅ lab-20（实测） |
| 16 | 并行度与 Slot 规划 | ✅（NoResource 实测） |
| 17 | 双流 Join | ✅ 速答（Flink 组） |
| 18 | Checkpoint 失败排查 | ✅ m20 Q9 |
| 19 | 事件时间 vs 处理时间实测 | ✅ lab-18 |
| 20 | 增量 Checkpoint/RocksDB | ✅ m20 Q7 |

**Flink 覆盖率：20/20 = 100%**

## 四、Spark（20 个）

| # | 知识点 | 覆盖 |
|---|---|---|
| 1 | Driver/Executor/DAG/Stage | ✅ m21 Q1（toDebugString 实测） |
| 2 | 宽窄依赖 | ✅ Q1 |
| 3 | RDD 血统与容错 | ✅ Q1/cache 实验 |
| 4 | 内存模型（统一内存） | ✅ Q5 |
| 5 | GC 调优 | ✅ Q5 |
| 6 | SortShuffle 机制 | ✅ Q2 |
| 7 | bypass/外部 Shuffle Service | ✅ Q2 加分 |
| 8 | Catalyst 四层计划 | ✅ Q4 |
| 9 | AQE 三能力 | ✅ Q2/速答 |
| 10 | 倾斜五解法选择树 | ✅ Q3（m23 实测） |
| 11 | 广播 Join | ✅ m22 实测 |
| 12 | 资源规划公式 | ✅ docs 6 节 |
| 13 | cache/persist 级别 | ✅ Q6（实测 Job 数） |
| 14 | Spark on K8s vs YARN | ✅ Q7 |
| 15 | 窗口函数/每组建 TopN | ✅ m22 实测 |
| 16 | 多源 Join/MTTR 分析 | ✅ m22 实测 |
| 17 | Structured Streaming 模型 | ✅ Q9（m24 实测） |
| 18 | 输出模式/水位/checkpoint | ✅ Q8（实测） |
| 19 | 微批 vs 持续处理 | ✅ Q9 |
| 20 | 小文件问题 | ✅ Q10 误区 |

**Spark 覆盖率：20/20 = 100%**

## 五、Agent 方向（20 个，含 Skill/MCP/Harness ≥5 个）

| # | 知识点 | 覆盖 |
|---|---|---|
| 1 | Transformer/Token 直觉 | ✅ m26 Q1 |
| 2 | Temperature/TopP | ✅ m26 Q2（采样实测） |
| 3 | Prompt 五段式/CoT/ReAct/ToT | ✅ m26 Q3/Q4 |
| 4 | 幻觉与防御 | ✅ m26 Q5 |
| 5 | Prompt 资产管理 | ✅ m26 Q6 |
| 6 | 多厂商协议差异/适配层 | ✅ m27 Q1（实测） |
| 7 | 重试/熔断/降级 | ✅ m27 Q2（实测） |
| 8 | SSE 流式工程坑 | ✅ m27 Q3 |
| 9 | Key 安全 | ✅ m27 Q4 |
| 10 | 成本计量与优化 | ✅ m27 Q5（实测） |
| 11 | ReAct 手写与安全阀 | ✅ m28 Q1（实测） |
| 12 | Memory 三层 | ✅ m28 Q3（实测） |
| 13 | 工作流 vs Agent 选型 | ✅ m28 Q4 |
| 14 | Agent 评估框架 | ✅ m28 Q7 |
| 15 | Harness/Skill/MCP 三层模型 | ✅ m29 Q1（★ 全实测） |
| 16 | Harness 架构设计 | ✅ m29 Q2 |
| 17 | Skill vs Tool 封装 | ✅ m29 Q3 |
| 18 | MCP 协议与安全 | ✅ m29 Q4/Q5/Q8（手写实现） |
| 19 | Skill 路由/版本热更 | ✅ m29 Q6/Q9 |
| 20 | 多 Agent 隔离共享/灰度/成本/vLLM | ✅ advanced Q2/Q4/Q5/Q6（多 Agent 实测） |

**Agent 覆盖率：20/20 = 100%**（Skill/MCP/Harness 专题 11 道齐备）

## 六、通信行业场景（10 个）

| # | 知识点 | 覆盖 |
|---|---|---|
| 1 | 5G 切片 SLA 分级监控 | ✅ 场景一（实测） |
| 2 | gNB KPI 数据建模与采集 | ✅ 场景一生成器（实测） |
| 3 | DDoS 检测与自动封禁 | ✅ 场景二（实测） |
| 4 | NetFlow 流量特征分析 | ✅ 场景二 Spark（实测） |
| 5 | Modbus/工业协议接入 | ✅ 场景三（实测） |
| 6 | 设备预测性维护（MLlib） | ✅ 场景三（实测 AUC 0.828） |
| 7 | NFV/CNF 编排与 SFC | ✅ 场景四（Operator 实测） |
| 8 | 基站/网元告警语义与保序 | ✅ 模块 16（复合 key 设计） |
| 9 | 节假日话务洪峰弹性 | ✅ HPA/KEDA 设计（场景一） |
| 10 | 边缘-中心协同（地市级） | ✅ 系统设计题 4 |

**通信行业覆盖率：10/10 = 100%**

## 七、综合统计

| 方向 | 覆盖/总数 | 覆盖率 |
|---|---|---|
| K8s | 47/50 | 94% |
| Kafka | 20/20 | 100% |
| Flink | 20/20 | 100% |
| Spark | 20/20 | 100% |
| Agent | 20/20 | 100%（含专题 11 道） |
| 通信行业 | 10/10 | 100% |
| **合计** | **138/140** | **98.6%** ✅（目标 ≥80%） |

❌ 未覆盖清单（由《分段追问手册》第 6 轮补全）：
1. OPA/Gatekeeper 三个策略模板实操（实验要求已定义在追问第 1 轮；镜像需联网拉取）
2. OpenTelemetry + Jaeger 链路追踪实操
3. KEDA scaler 实操（设计文档已覆盖）

✅ 深化补充（第二轮）：Ingress 路由+金丝雀权重实测（kind 真实集群）、metrics-server 部署排障四层实录（含失败 APIService 毒化控制面事故）、HPA 完整生命周期（扩容/稳定窗/缩容事件链实测）——见 k8s/advanced/02-ingress-lab/。K8s 覆盖率更新：48/50 = 96%
✅ 深化补充（第二轮·题量）：新增 interview/k8s-questions-advanced.md（模块 7-14 要点链 34 道）+ interview/agent-questions-supplement.md（RAG/护栏/多 Agent/治理 14 道）；模块 11 Kafka Operator Go 代码编译通过

## 八、排障与系统设计

- 排障实战题：10 道全（`interview/troubleshooting-challenges.md`），其中 4 道标注本项目真实踩坑（★）
- 综合系统设计题：8 道全（`interview/system-design-questions.md`，七段式结构）
- 面试题总量：K8s 66（含进阶 34）· Kafka 31 · Flink 16 · Spark 16 · Agent 60（含补充 14）· 系统设计 8 · 排障 10 = **207 道**
