# Lab：metrics-server 部署排障 + HPA 完整生命周期（kind 真实集群，已实测）

> 对应模块 10（HPA）与模块 12（集群排障）。**本 lab 的排障过程就是最高价值的教学内容。**

## 实验一：metrics-server 部署（排障全程实录）

精简部署 metrics-server 后连续踩了四层问题，每一层都是生产真实形态：

| # | 现象 | 根因 | 修复 |
|---|---|---|---|
| 1 | `mkdir apiserver.local.config: permission denied` | 自签证书写 CWD（只读） | `--cert-dir=/tmp` |
| 2 | `listen on 0.0.0.0:443: permission denied` | 非 root（65534）不能绑 <1024 端口 | `--secure-port=10250`（Service targetPort 对齐） |
| 3 | `configmaps extension-apiserver-authentication forbidden` | 聚合 API 需读请求头 CA 的 configmap | RoleBinding（kube-system 内）+ ClusterRoleBinding `system:auth-delegator` |
| 4 | ★ **controller-manager / scheduler 连锁 CrashLoop（leaderelection lost）** | 失败的 APIService 让聚合发现（discovery）报 stale GroupVersion，CM/Scheduler 的 informer 与选主请求被拖死 | **删除 APIService 先恢复控制面**，修复后重新注册 |

**模块 12 排障方法论实证**：
1. 一个"不起眼"的组件失败（metrics-server）可以**毒化整个控制面**——聚合 API 的 discovery 失败会拖垮所有依赖它的组件；
2. 控制面排障优先级：先恢复（删除毒源）→ 再修复（补 RBAC/重新注册）；
3. 静态 Pod 的 CrashLoopBackOff 退避卡在 kubelet，`docker restart <node>` 是重置手段；
4. 恢复验证：`kubectl top nodes` 正常输出。

## 实验二：HPA 完整生命周期（扩容 → 稳定窗 → 缩容）

```bash
kubectl apply -f hpa-demo.yaml    # busyloop 负载 + requests.cpu=100m + HPA 目标 50%
```

**实测事件链**（kubectl describe hpa 的 Events 是评分依据）：

```
[负载运行]   cpu: 200%/50%（request 100m，实际 201m）
[约 90 秒后] Normal  SuccessfulRescale  New size: 4; reason: cpu resource utilization above target
[负载移除]   cpu: 0%/50%，副本维持 4 —— 缩容稳定窗（默认 300s）生效 ★
[3 分钟后]   Normal  SuccessfulRescale  New size: 1; reason: All metrics below target
```

**三个实测确认的设计细节**：
1. 扩容无稳定窗（快速响应）、缩容 300s 窗（防抖动）——HPA 的 `behavior` 字段可调（场景一 hpa.yaml 有配置实例）；
2. 期望副本公式：`ceil(1 × 200/50) = 4`（受 maxReplicas=4 截断）；
3. 早期 `FailedGetResourceMetric` 警告是指标管道（kubelet→ms→HPA）的冷启动延迟，属正常。

## 收获对照（模块 10 考点全覆盖）

- HPA 数据源链路（kubelet cAdvisor → metrics-server → metrics.k8s.io → HPA）✓
- 算法/上下限/稳定窗 ✓  · VPA 冲突理解加深（HPA 依赖 requests，VPA 会改 requests）✓
