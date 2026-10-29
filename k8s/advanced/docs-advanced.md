# K8s 进阶层（模块 7-14）知识文档（压缩版）

> **优先级说明**：本层按执行协议为 P2（可简化）——每模块给出核心考点、关键命令与实验方向，深度展开由《分段追问手册》第 1 轮按需补充。
> **预估学习时间**：模块 7-14 共 24 小时

---

## 模块 7：Helm 与 GitOps

**Chart 结构**：Chart.yaml（元数据）/ values.yaml（默认值）/ templates/（模板）/ charts/（依赖）。本项目的参数化实例：`telecom/5g-slice-monitor/deploy/helm/`。

**核心考点**：
- 模板函数：`toYaml|nindent 12`（缩进陷阱）、`required`（强制值）、`lookup`（读集群现状）；
- Hook：`helm.sh/hook: pre-install/post-upgrade`（数据库迁移/测试）；`--wait` 与 hook 权重；
- Kustomize 对比：无模板、纯 patch 层叠（base + overlays），适合"同构多环境微调"；Helm 适合"参数化发新版本"；
- ArgoCD：Git 仓库为事实源，Sync 状态机（OutOfSync→Syncing→Healthy），ApplicationSet 批量生成（多集群/多环境），Argo Rollouts 渐进式交付（金丝雀分析）。

**实验方向**：把场景一 Helm Chart 部署进 kind；再写 ArgoCD Application 指向它（ArgoCD 需额外安装）。

## 模块 8：RBAC 与安全体系

- **RBAC 四件套**：Role（ns 内）/ClusterRole（全集群）/RoleBinding/ClusterRoleBinding；最小权限实例：只读部署权限 = `get/list/watch deployments`（无 create/delete）；
- **ServiceAccount Token 投影**（1.24+）：bound token 有过期， Pod 内 `volumeMounts` 自动注入；
- **PSA（Pod Security Admission）**：ns 级标签 `pod-security.kubernetes.io/enforce: restricted`——替代废弃的 PSP；
- **OPA/Gatekeeper**：ConstraintTemplate（Rego 策略）+ Constraint（实例）——镜像来源限制/禁止特权容器/资源配额强制的三个经典模板（追问手册第 1 轮有完整实现）；
- **镜像安全**：Cosign 签名 + 准入验证、Trivy 扫描（CI 卡点）；
- **等保合规视角**：最小权限 + 审计日志（audit-policy）+ 网络隔离（NetworkPolicy）+ 密钥管理（KMS encryption at rest）。

## 模块 9：可观测性体系

- **Prometheus Operator**：CRD 化监控配置——ServiceMonitor（发现抓取目标）/PrometheusRule（告警规则）/Alertmanager CR；
- **PromQL 高阶**：`rate()`（计数器）、`histogram_quantile()`（P99）、`increase()[5m]`、子查询与 `@ modifier`；
- **Grafana**：本项目 provisioning 实例（datasource+dashboard 即代码，`docker/monitoring/grafana/`）；
- **日志**：EFK（重检索、成本高）vs Loki（标签索引、成本低、LogQL）——日志量大的网管平台倾向 Loki+对象存储；
- **链路追踪**：OpenTelemetry SDK → Collector → Jaeger/Tempo；采样策略（头部采样 vs 尾部采样——保留慢/错请求）；
- **通信场景**：设备监控大盘设计（模块 15-20 的 Kafka/Flink/ES 指标已接 Prometheus，见 `docker/monitoring/prometheus/prometheus.yml`）；告警降噪：分组（group_by）、抑制（告警风暴时抑制子告警）、静默（维护窗口）。

## 模块 10：HPA/VPA/KEDA 弹性伸缩

- **HPA 算法**：`期望副本 = ceil(当前副本 × 当前指标/目标指标)`；抖动治理：stabilizationWindow（缩容 300s）、behavior 策略（本项目场景一 hpa.yaml 实例）；
- **VPA**：推荐/自动调 requests——与 HPA 同指标冲突（CPU-HPA 与 VPA 的 Off 模式只给建议）；
- **KEDA**：事件驱动伸缩——Kafka lag 触发消费者扩容（`kafka.consumer-group` scaler）是有状态流作业的正确姿势（对比：Flink 作业本身不能 HPA，需 Savepoint 迁移）；
- **通信场景**：节假日话务洪峰——生成器 HPA（场景一）+ KEDA 按 Lag 伸缩消费者 + 告警风暴预案（降采样）。

## 模块 11：Operator 开发

**核心模式**（场景四 Go Operator 全部实证）：
- Reconcile 必须幂等（AlreadyExists 视为成功）；只做"声明→实际"的收敛，不做一次性动作；
- Finalizer：删除前清理外部资源（摘流/释放 IP），失败则重试不放行删除；
- Status 子资源：与 spec 分离，`kubectl get cr` 可读；事件（Recorder）记录关键动作；
- 测试：envtest（本地 apiserver+etcd）单元测试 Reconcile（追问手册第 1 轮含 Kafka Operator 完整实现与 envtest）。

## 模块 12：集群运维实战

- **证书轮换**：kubeadm 集群一年期证书，`kubeadm certs renew all` + 控制面逐台滚动；
- **etcd 备份恢复**：`etcdctl snapshot save/restore`（恢复=新集群种子数据）；演练季度化；
- **蓝绿升级**：新控制面集群 → 迁工作负载 → 切流量；节点排水（drain + PDB）；
- **NotReady 排查法**：kubelet 状态 → CNI 插件 → 容器运行时 → 磁盘压力 → 系统资源（mem/cpu/pid）；
- **CrashLoopBackOff**：logs --previous → 探针配置 → 依赖服务 → 资源限制；
- **ImagePullBackOff**：镜像名/凭证（imagePullSecrets）/仓库可达性/节点缓存（kind load）。

## 模块 13：Service Mesh（Istio）

- **数据面**：Envoy sidecar 注入（istio-injection=enabled），流量劫持 iptables→Envoy；
- **VirtualService/DestinationRule**：金丝雀（subset 权重 90/10）、流量镜像（mirror）、超时重试熔断；
- **mTLS**：对等认证（PeerAuthentication STRICT）+ 网格证书轮换（istiod CA）；
- **代价**：sidecar 资源开销与延迟（每跳 +1ms 量级）；Cilium eBPF mesh 是无 sidecar 方向；
- **通信场景**：核心网元间 mTLS（等保要求）+ 灰度发布（Agent 平台金丝雀，系统设计题 3）。

## 模块 14：多集群与混合云

- **Cluster API**：声明式建集群（Cluster/Machine CRD），多 Provider 抽象；
- **跨集群服务发现**：多 Primary（Istio）、Submariner/ServiceImport、Federation v2（策略分发）；
- **通信场景**：省-地市两级集群（边缘自治+中心管控）、边缘-中心协同（K3s 边缘节点 + K8s 中心，应用分层部署）；
- **选型**：管控面集中 vs 自治——断网自治能力是边缘场景的硬要求（本地缓存+本地决策）。
