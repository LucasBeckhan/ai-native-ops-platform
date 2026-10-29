# 场景四：NFV 网络服务编排平台

> **融合点**：K8s CRD/Operator（模块 11 应用）→ 服务链（SFC）编排 → Skill 封装 → Agent 辅助
> **预估学习时间**：6 小时 | **优先级**：P1
> **验证状态**：Operator 已编译（go build 51MB）并在 kind 集群实测 Reconcile 全流程

## 架构与实测记录

```
┌─ 声明层 ────────────────────────────────────────────────────┐
│ NetworkServiceChain CR（用户/上层编排声明期望服务链）：       │
│   edge-sfc-01: ingress=edge-classifier-a                     │
│   跳序: firewall(2副本) → ids(2副本) → loadbalancer(3副本)   │
└──────────────┬──────────────────────────────────────────────┘
               ▼ Reconcile 循环（operator/main.go）
┌─ 编排层（NFV Operator，go build 51MB，kind 集群实测）────────┐
│ 1. Finalizer：删除 CR 前先摘流量（drainTraffic，防黑洞）      │
│ 2. 逐跳幂等创建 Deployment（edge-sfc-01-firewall-0 等 3 个）  │
│ 3. Status 上报：Phase=Provisioning / Serving，ReadyHops      │
│    实测：kubectl get nsc → Provisioning 0/3                  │
│            kubectl get deploy → 3 跳 Deployment 全部创建      │
└──────────────┬───────────────────────────────────────────────┘
               ▼
┌─ 智能层（Skill 封装）────────────────────────────────────────┐
│ 服务链编排 Skill：SFC 创建→验证→上线三段工作流                │
│ Agent 辅助：配置推荐（基于历史链路模板）/ 异常检测与自愈建议    │
│ 写操作（链路上线）走 Harness 权限矩阵（审批制，同场景二）      │
└──────────────────────────────────────────────────────────────┘
```

## 关键技术点（面试价值）

1. **Finalizer 模式**：CR 删除是异步的——先摘流再放行删除，摘流失败则重试不放行（实测代码路径）；
2. **幂等 Reconcile**：Create 返回 AlreadyExists 视为成功；升级时校准 spec 漂移；
3. **Status 子资源**：`kubectl get nsc` 直接看 Provisioning/Serving 与 ReadyHops（additionalPrinterColumns）；
4. **SFC 语义**：跳序=流量路径；Ingress 分类器把流量引入链首；生产对接 SDN 控制器下发导流规则；
5. **VNF/CNF 生命周期**：实例化（创建 Deployment）→ 扩缩（replicas 收敛）→ 升级（RollingUpdate）→ 终止（Finalizer 摘流+级联删除）。

## 文件清单

| 文件 | 说明 | 验证状态 |
|---|---|---|
| operator/main.go | Reconcile/Finalizer/Status/幂等创建 | ✓ 编译+kind 实测 |
| operator/api/v1/{types.go, groupversion_info.go, deepcopy.go} | CRD 类型与 scheme 注册 | ✓ 编译 |
| operator/manifests/crd.yaml | CRD 定义 + 示例服务链实例 | ✓ kind 实测 |
| operator/go.mod | 依赖（controller-runtime 0.17） | ✓ goproxy.cn 可拉取 |

## 快速开始

```bash
cd telecom/nfv-orchestrator/operator
GOPROXY=https://goproxy.cn,direct go build -o /tmp/nfv-operator .
kubectl apply -f manifests/crd.yaml          # CRD + 示例实例
/tmp/nfv-operator &                          # 运行（默认读当前 kubeconfig）
kubectl get nsc edge-sfc-01                  # 实测: Provisioning 0/3
kubectl get deploy | grep edge-sfc           # 实测: 3 跳 Deployment 创建
# VNF 镜像本机不存在 → Pod ImagePullBackOff 属预期（编排层逻辑已验证）
```
