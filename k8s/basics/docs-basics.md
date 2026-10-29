# K8s 基础层（模块 1-6）知识文档

> **实验环境**：kind 集群（`kind create cluster --name teleops --image kindest/node:v1.31.4`），全部实验已在真实集群验证，预期输出为实测记录。
> **预估学习时间**：模块 1-6 共 24 小时 | **优先级**：P1

---

## 模块 1：集群架构深度剖析

### 1.1 控制面组件协作（kind 单节点即完整控制面）

本集群 kube-system 实测组件：`etcd / kube-apiserver / kube-controller-manager / kube-scheduler / coredns / kube-proxy / kindnet(CNI)`。

```
kubectl apply ──► kube-apiserver（唯一入口：认证/鉴权/准入）
                    │ 写入
                    ▼
                  etcd（唯一事实源，Raft 共识）
                    │ watch（list-watch 机制）
                    ├──► kube-controller-manager：期望态 vs 实际态 → 逐个控制器收敛
                    │     （Deployment→RS→Pod 层层控制，声明式的心脏）
                    └──► kube-scheduler：未调度 Pod → 预选(过滤)→优选(打分)→绑定
                             │ 写 binding
                             ▼
                    kubelet（节点上）watch 到绑定 → CRI 拉镜像起容器 → 上报状态
```

**list-watch 是 K8s 声明式的底层机制**：组件不轮询，而是 watch 资源事件流（etcd 的高效 watch 由 `--experimental-watch-progress-notify-interval=5s` 等参数支撑，本集群 etcd 实测启动参数已含 corrupt check）。

### 1.2 etcd 与 Raft（面试一级考点）

- **Raft 三角色**：Leader（处理写）、Follower（复制）、Candidate（选举中）；任期（term）单调递增防脑裂；
- **写流程**：客户端 → Leader 追加日志 → 复制给多数派 → 提交并应用 → 响应；少数派宕机不阻塞，**多数派故障则集群不可写**（保一致性弃可用性）；
- **K8s 实践**：奇数节点（3/5）、`--experimental-initial-corrupt-check`（启动校验，本集群已开）、etcd 碎片整理（defrag）与快照备份（`etcdctl snapshot save`）；
- **性能**：etcd 承受每秒数千写入即上限——大集群的优化方向是减少 watch 压力与对象数量（ARR：API Priority and Fairness 保护 apiserver）。

### 1.3 kube-proxy 三模式（本集群实测 `mode: iptables`）

| 模式 | 原理 | 性能 |
|---|---|---|
| iptables | 规则链随机概率负载均衡 | 规则线性增长，服务多时延迟高 |
| **IPVS** | 内核哈希表+真实 LB 算法 | 大规模集群标配（万级 Service） |
| eBPF（Cilium 替代 kube-proxy） | 数据面编程 | 最快+可观测 |

大集群从 iptables 切 IPVS 的动因：iptables 更新是 O(n) 全量刷规则，IPVS 增量更新哈希表。

### 1.4 高可用架构

三 Master（堆叠 etcd）+ LB；证书体系（kubeadm 一年期证书与轮换 `kubeadm certs renew`）；脑裂防护（leader-elect 默认开启）。

---

## 模块 2：核心资源对象

### 2.1 Pod 与三探针组合（实测 describe 输出）

```
Startup:   http-get :80  period=2s  failure=30   ← 慢启动窗口：通过前不跑另外两个探针
Liveness:  http-get :80  period=10s failure=3    ← 失败重启容器（自愈，误杀风险）
Readiness: http-get :80  period=5s  failure=3    ← 失败摘除 Endpoints（不重启不接流）
```

**设计哲学**：liveness 管"活着"（该重启的重启），readiness 管"能接活"（没绪别把流量打过来），startup 管"给它时间"（防止慢启动被 liveness 误杀）。Java 大堆应用：startupProbe.failureThreshold × period ≥ JVM 启动时间。

### 2.2 Deployment 发布与回滚（实测全记录）

```bash
kubectl set image deployment/web nginx=busybox:1.36   # 灌入"坏镜像"（busybox 无 nginx 进程）
kubectl rollout status ...   # 卡在 0/3 —— 新 Pod CrashLoopBackOff，旧 RS 保留（maxSurge 并发）
kubectl rollout undo deployment/web                # 一键回滚
kubectl rollout status ...   # deployment "web" successfully rolled out（3/3 Running）✓实测
```

关键机制：滚动更新是**新旧 ReplicaSet 并存**（maxSurge/maxUnavailable 控制节奏），`revisionHistoryLimit` 保留可回滚版本；`kubectl rollout undo` 把 RS 的 template 换回去。

### 2.3 Service 与 Endpoints（实测：Endpoints 自动维护）

`kubectl expose` 后 Endpoints 控制器把**就绪** Pod IP 填入（readiness 失败自动摘除——与探针联动）。三种类型：ClusterIP（默认）、NodePort、LoadBalancer（kind 用 metallb 方可实验）。

---

## 模块 3：网络模型

**CNI 分工**：kindnet（kind 默认）只提供 Pod 网络与路由，**不实现 NetworkPolicy**——这是实验环境的重要事实（Policy 需 Calico/Cilium）。生产 CNI 选型：Calico（BGP 大规模/网络策略成熟）vs Cilium（eBPF 性能/可观测）。

**Service 发现链路**：Pod → CoreDNS（`my-svc.ns.svc.cluster.local` → ClusterIP）→ kube-proxy 规则 → Pod IP。CoreDNS 调优：缓存、`ndots:5` 带来的额外解析（外部域名搜索域展开）——通信行业大集群常见优化点。

**NetworkPolicy 设计模式**（生产清单）：默认拒绝所有入站 → 按调用链放行（白名单式）；egress 管控 DNS+目标服务；策略是"加法叠加"的（选中的 Pod 上所有 Policy 并集生效）。

---

## 模块 4：存储体系

### 4.1 PV/PVC/StorageClass 三层（实测静态供给）

```
实测：PV(1Gi, hostPath, manual) ◄──Bound── PVC(512Mi, manual)
静态供给：管理员预建 PV，PVC 按名字类匹配（教学直观）
动态供给：StorageClass + CSI 驱动按需建 PV（生产主流）
回收策略：Retain（保留数据人工处理）/ Delete（动态供给默认）/ Recycle（废弃）
```

**CSI 架构**：Provisioner(建卷)/Attacher(挂节点)/NodePlugin(格式化挂载) 三类 Pod 分离，与 K8s 解耦演进。

### 4.2 通信场景选型

日志类：本地盘 + DaemonSet 采集（Fluent Bit/Loki）——日志不需要强持久化；Flink Checkpoint：网络存储（EBS/CFS）保证节点漂移可恢复；数据库类：块存储 + 拓扑感知（zone 绑定）。

---

## 模块 5：调度机制

### 5.1 调度两阶段

**预选（Filter）**：硬约束——资源够不够、端口冲突、nodeSelector/nodeAffinity 硬条件、污点容忍、卷拓扑；**优选（Score）**：软偏好打分——资源均衡（LeastRequestedPriority）、亲和性、镜像本地性、Pod 反亲和打散。

### 5.2 亲和、污点与优先级（实测 nodeSelector）

```bash
kubectl label node teleops-control-plane node-type=core
# Pod nodeSelector: node-type=core → 调度成功（实测 Running）✓
```

| 机制 | 语义 | 通信场景 |
|---|---|---|
| nodeSelector/亲和 | Pod→节点选择 | 核心网元固定到专属节点池 |
| Pod 反亲和 | 分散副本 | 基站网关跨机架打散（故障域隔离） |
| 污点+容忍 | 节点排斥 Pod | `node.kubernetes.io/unreachable` 自动驱逐；专用节点 taint 隔离 |
| PriorityClass+抢占 | 高优先级挤低优先级 | 核心网监控 > 普通分析作业（电信业务分级） |

---

## 模块 6：命名空间与资源管理（实测 ResourceQuota/QoS）

```bash
# 实测：quota-ns 内 Pod 创建后配额计数
pods: 1/3   requests.cpu: 100m/500m   limits.memory: 64Mi/1Gi
# LimitRange：未写 resources 的容器自动注入 default/defaultRequest
```

**QoS 三级**（kubectl get pod -o jsonpath={.status.qosClass} 实测）：

| QoS | 条件 | OOM 时命运 |
|---|---|---|
| Guaranteed | requests==limits 且 CPU/MEM 全设 | 最后被杀 |
| Burstable | 设了 requests≠limits 或部分设置 | 按 usage/requests 比例 |
| BestEffort | 全不设 | 第一个被杀 |

**OOM 排查路径**：`kubectl describe pod` 看 `OOMKilled` 退出码 137 → 容器 limits.memory < 实际使用 → 调 limits 或查内存泄漏；节点级 OOM 看 `dmesg | grep -i oom`。

**多租户配额设计**（通信场景：核心网/支撑网/测试网分命名空间）：Namespace → ResourceQuota（对象数+资源总量）→ LimitRange（默认值+上下限）→ PSA（安全基线）四件套。

## 自检清单（每条都能展开讲 2 分钟即过关）

1. list-watch 机制与声明式收敛的关系
2. etcd Raft 写流程与多数派语义
3. iptables→IPVS 的演进动因
4. 三探针组合设计（Java 慢启动案例）
5. 滚动更新与回滚的 RS 机制
6. kindnet 不支持 NetworkPolicy 的实验事实与生产 CNI 选型
7. PV/PVC/StorageClass/CSI 四个角色
8. 预选优选清单
9. QoS 三级与 OOM 排查
10. 多租户四件套
