# =====================================================================
# 文件名: k8s/advanced/11-operator/kafka-operator/README.md
# 说明: Kafka Operator 的构建/部署/测试步骤（模块 11 实验入口）
# =====================================================================

# Kafka Operator（模块 11）

管理 Kafka 集群的声明式 Operator：`KafkaCluster` CRD（Broker 数/镜像/存储/声明式 Topic）。

## 代码结构

```
kafka-operator/
├── main.go                        # 入口：scheme 注册 + controller 启动
├── api/v1/
│   ├── types.go                   # KafkaCluster CRD 类型（Spec/Status）
│   ├── groupversion_info.go       # scheme 注册（kubebuilder 模式）
│   └── deepcopy.go                # 手写深拷贝（生产由 deepcopy-gen 生成）
├── controllers/controller.go      # ★ Reconcile 逻辑（三步收敛，详见文件头注释）
└── manifests/                     # CRD YAML + 示例实例（补全练习）
```

## 构建与运行

```bash
cd k8s/advanced/11-operator/kafka-operator
GOPROXY=https://goproxy.cn,direct go build -o kafka-operator .

# 前提：kind 集群可用（见 docs/environment-prep.md 2.3）
kubectl apply -f manifests/          # CRD（示例实例需自行创建，见下）

./kafka-operator &                   # 运行（读当前 kubeconfig）
kubectl get kafkacustomclusters -A   # 观察 Status.Phase 从 Provisioning → Ready
```

## 声明式 Topic 的实验（理解"只增不减"防线）

1. CR 的 `spec.topics` 声明 topic-a；
2. Apply 后观察 `kafkacluster-topics` ConfigMap 生成对应条目；
3. **从声明中删除 topic-a 再 apply**——ConfigMap 中条目不消失，而是追加
   `# 已从声明移除，人工确认后删除` 标记：Kafka 分区/数据不可随意删除，
   Operator 永远不自动做破坏性动作（生产安全设计，面试考点）。

## Reconcile 逻辑讲解顺序（面试走读）

1. Finalizer（删除前提示数据归档）
2. `ensureBrokerStatefulSet`：Headless Service + KRaft quorum voters 自动生成
   + PVC 模板 + **只扩不缩**（缩 Broker 需迁移分区，不自动）
3. `reconcileTopics`：声明式对齐 + 破坏性动作拦截
4. Status 上报（Provisioning/Ready/Degraded）

## envtest 单元测试（待补全，追问第 1 轮）

框架骨架（补全练习）：`controllers/controller_test.go`

```go
// envtest 要点：
// 1. testenv.StartEnv() 启动本地 apiserver+etcd（需 setup-envtest 下载二进制）
// 2. k8sClient := ctrl.GetClientOrDie()，用 WithIndex 注册 pod-index
// 3. 测试用例：创建 KafkaCluster(brokers=3) → 等 StatefulSet 创建 →
//    校验 replicas/voters 字符串 → 更新 status.readyBrokers=3 → 校验 Phase=Ready
// 4. 声明式 Topic 的"只增不减"用例：删除声明中的 topic → 断言 ConfigMap 保留条目
```

安装与运行：

```bash
go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
setup-envtest use 1.31.x
KUBEBUILDER_ASSETS=$(setup-envtest use 1.31.x -p path) go test ./controllers/...
```
