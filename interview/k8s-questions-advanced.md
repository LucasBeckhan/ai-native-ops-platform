# K8s 进阶层面试题（模块 7-14，30 道）

> 第二轮深化补充。每道题给出**答题要点链**（口头面试按要点展开即可）；
> 深度解析的元分析结构同基础层（`k8s/basics/interview-basics.md`）。

## 模块 7：Helm 与 GitOps（6 道）

1. **Helm Hook 的执行时机与删除策略？**
   要点：pre-install/post-install/post-upgrade 等锚点 → Job 完成才继续（hook-delete-policy: before-hook-creation/hook-succeeded）；典型用途=数据库 schema 迁移。坑：hook 资源不属于 release（helm uninstall 不删，需 policy）。
2. **Helm 与 Kustomize 怎么选？**
   要点：参数化发版用 Helm（多租户多配置）；同构微调用 Kustomize（无模板、纯 patch、diff 友好）；两者可组合（Kustomize 引用 helm template 产物）。
3. **values 的优先级链？**
   要点：`-f` 多文件后者覆盖前者 → `--set` 最高；`required` 函数强制必填防"零值悄悄生效"。
4. **ArgoCD 的 Sync 状态机与健康判定？**
   要点：OutOfSync→Syncing→Synced/Healthy；健康判定可自定义（Lua）；Sync 策略 automated+prune+selfHeal 三开关的语义与风险（prune 误删）。
5. **Argo Rollouts 与原生 Deployment 金丝雀的区别？**
   要点：Rollouts CRD 支持按步进比例+分析器（Prometheus 指标门禁自动回滚）；原生 Deployment 只有 maxSurge 粗粒度。对照：本项目 Ingress canary-weight 实验（9/100 精确分流）就是 Rollouts 流量层的底座。
6. **GitOps 的"漂移"怎么处理？**
   要点：selfHeal 自动拉回 vs 告警人工决策；允许漂移的例外（HPA 动态副本）——ArgoCD ignoreDifferences 配置。

## 模块 8：RBAC 与安全（6 道）

7. **最小权限的 Role 设计流程？**
   要点：先 audit（operator 视角记录真实 API 调用）→ 聚合为 verbs → Role/Binding 拆分（ns 级职责用 Role，跨 ns 用 ClusterRole+RoleBinding per-ns）。
8. **ServiceAccount Token 的演进（1.24+）？**
   要点：bound token（有时效+audience）替代永久 secret；TokenRequest API；投影卷自动挂载。
9. **PSA 三档（privileged/baseline/restricted）的差异？**
   要点：restricted 禁 hostNetwork/特权/未授权 capability；enforce/audit/warn 三模式并行（灰度收紧）。替代废弃的 PSP。
10. **OPA/Gatekeeper 与 PSA 的分工？**
    要点：PSA=K8s 内置基线（够用但不可编程）；Gatekeeper=可编程策略（ConstraintTemplate Rego）+审计报表。三个经典模板：镜像前缀白名单、禁特权容器、强制 requests/limits。
11. **镜像供应链安全链路？**
    要点：CI 构建 → Trivy 扫描卡点 → Cosign 签名 → 准入验证（cosign verify）→ 运行时扫描（Kubesec/持续扫描）。缺一环的后果。
12. **等保/合规视角的 K8s 加固清单？**
    要点：RBAC 最小权限、etcd 加密（KMS）+审计日志、NetworkPolicy 零信任、镜像签名、控制面隔离、密钥轮换——每项对应的等保条款思路。

## 模块 9：可观测性（4 道）

13. **Prometheus Operator 相比手工配置的优势与代价？**
    要点：CRD 化（ServiceMonitor/PrometheusRule）=声明式+GitOps；代价= Operator 本身运维+CRD 版本迁移。
14. **PromQL 写"近 1 小时错误率突增检测"？**
    要点：`rate(http_errors[5m]) / rate(http_total[5m])` 比值 + `avg_over_time` 平滑 + 阈值持续判定（`for:`）；反例：直接对 counter 求 avg。
15. **Loki 与 EFK 的本质差异？**
    要点：Loki 只索引标签（成本低，查询靠标签过滤+内容 grep）；EFK 全文索引（查询快、存储贵）。日志量大且查询模式固定→Loki。
16. **告警降噪三板斧？**
    要点：分组（group_by 维度聚合成一条通知）、抑制（ critical 触发时抑制同源 warning）、静默（维护窗口）；再加"告警分级路由"（P0 电话/P2 群消息）。对照模块 9 大盘设计。

## 模块 10：弹性伸缩（4 道）

17. **HPA 的指标冷启动与"no metrics"处理？**
    要点：metrics-server 冷启动期间 FailedGetResourceMetric 是正常（本项目实验实测）；Pod 刚启动无样本→HPA 跳过该轮；`--v` 调试指标链路。
18. **缩容为什么比扩容保守？**
    要点：300s 稳定窗（防抖动）+ 逐级缩容 policy；本项目实测：负载移除后约 5 分钟才缩（事件链已记录）。
19. **KEDA 的 scaler 模型与 HPA 的关系？**
    要点：KEDA 底层仍是 HPA（生成 ScaledObject→HPA），增量在"事件源指标"（Kafka lag/队列深度）+ scale to zero。Flink 作业不能随意伸缩（Savepoint 迁移），KEDA 适合无状态消费者。
20. **VPA 与 HPA 同用会怎样？**
    要点：同指标（CPU）互相打架——VPA 改 requests 会即时重算 HPA；要么指标错开（HPA 用自定义吞吐指标），要么 VPA Off 模式只给建议。

## 模块 11：Operator（4 道）

21. **Reconcile 必须幂等的具体含义？**
    要点：任意时刻、任意次数执行结果一致——Create 的 AlreadyExists 视为成功（本项目 Kafka/NFV Operator 双实证）；不做"增量补丁"逻辑；状态外置（Status 子资源）。
22. **Finalizer 的正确实现？**
    要点：创建时加 finalizer → DeletionTimestamp 非空时执行清理 → 成功后移除 finalizer → K8s 才真正删除；清理失败必须返回 error 阻止删除（宁可不删不可漏清）。本项目摘流场景实证。
23. **为什么 Status 与 Spec 分离为子资源？**
    要点：并发写冲突隔离（spec 由用户/编排写、status 由控制器写）；`kubectl get cr` 打印列（printcolumn）提升运维体验。
24. **Operator 的测试策略？**
    要点：envtest（真实 apiserver+etcd 二进制，无 kubelet）单测 Reconcile；fake client 的局限（无索引/无子资源语义）；集成测试用 kind 全链路。追问第 1 轮有 Kafka Operator 的 envtest 完整要求。

## 模块 12：集群运维（4 道）

25. **kubeadm 集群一年期证书到期的滚动轮换？**
    要点：`kubeadm certs check-expiration` → renew all → 逐台控制面重启静态 Pod；提前 30 天告警。
26. **etcd 快照恢复的完整步骤与坑？**
    要点：snapshot save → restore（生成新集群种子数据）→ 替换数据目录 → 起集群；坑：恢复后 apiserver 证书绑定 SAN、成员列表重置。
27. **节点排水（drain）与 PDB 的联动？**
    要点：drain 触发自愿驱逐 → PDB 拒绝超限驱逐 → 升级安全；PDB 配 0 时 drain 卡死（半故障态）。
28. **升级控制面的风险控制？**
    要点：etcd 先备份 → 逐台（等 Ready）→ kubeadm upgrade apply 顺序；版本最多跨一个 minor；canary 节点先升。

## 模块 13：Service Mesh（3 道）

29. **Sidecar 注入后流量路径？**
    要点：iptables TPROXY/REDIRECT 把出入流量劫持到 Envoy（15006 入/15001 出）→ mTLS → 上游；一跳两次代理的延迟模型（对照排障题 8）。
30. **VirtualService/DestinationRule 联动实现金丝雀？**
    要点：DR 定义 subset（v1/v2 标签）+ VS 按 weight 分流；与 Ingress canary 注解的差异（mesh 内更细：按 header/用户）。
31. **mTLS 的证书体系与轮换？**
    要点：istiod 作为 CA，workload 证书 24h 轮换自动；PeerAuthentication STRICT 强制；与外部 CA（Cert-Manager）集成。

## 模块 14：多集群（3 道）

32. **Cluster API 的声明式建集群流程？**
    要点：Cluster/Machine CRD → Provider 控制器（CAPA/CAPZ…）创建基础设施 → bootstrap 生成节点 → join；管理集群与工作负载集群分离。
33. **跨集群服务发现的方案对比？**
    要点：Istio 多主（共享 Root CA + 远程网关）、Submariner（隧道直通 Pod IP）、K8s Federation v2（策略分发）——按网络连通性与信任域选择。
34. **边缘自治的硬要求？**
    要点：断网时本地决策（K3s+本地缓存）、配置 pull 模式、数据缓存补传——中心化的 GitOps 在断网场景会失效，必须边缘拉取。

---
**总量说明**：K8s 方向 = 基础层 12 道完整解析 + 本文件 34 道要点链 + 速答 20 道 = 66 道；覆盖模块 1-14 全部考点。
EOF
echo "k8s advanced interview written"