# Lab：K8s 基础层六连（kind 真实集群，全部实测）

> **前置**：kind + kubectl 已装；`kind create cluster --name teleops --image kindest/node:v1.31.4`
> **预计耗时**：120 分钟

## 实验一：控制面观察（模块 1，15 分钟）

```bash
kubectl get pods -n kube-system
# 实测: coredns×2 / etcd / kube-apiserver / kube-controller-manager /
#       kube-scheduler / kube-proxy / kindnet
kubectl -n kube-system get pod etcd-teleops-control-plane -o jsonpath='{.spec.containers[0].command}'
# 实测: 含 --experimental-initial-corrupt-check=true（etcd 启动校验）
kubectl -n kube-system get cm kube-proxy -o yaml | grep "mode:"
# 实测: mode: iptables（切换 IPVS 的方法见文档 1.3）
```

## 实验二：发布、故障与回滚（模块 2，25 分钟）

```bash
kind load docker-image nginx:1.27 busybox:1.36 --name teleops   # ★ 离线集群必做
kubectl create deployment web --image=nginx:1.27 --replicas=3
kubectl set image deployment/web nginx=busybox:1.36    # 灌入坏镜像
kubectl get pods -l app=web     # 预期: 新 Pod ImagePullBackOff/CrashLoopBackOff
kubectl rollout undo deployment/web && kubectl rollout status deployment/web
# 实测: deployment "web" successfully rolled out（3/3 Running）
```

三探针部署见 `k8s/basics/02-core-resources/`（probe-demo manifest），验证：
```bash
kubectl describe pod -l app=probe-demo | grep -E "Liveness|Readiness|Startup"
kubectl expose deployment probe-demo --port=80 && kubectl get endpoints probe-demo
# 实测: ENDPOINTS 10.244.0.10:80（readiness 通过才进 Endpoints）
```

## 实验三：存储静态供给（模块 4，15 分钟）

```bash
kubectl apply -f k8s/basics/04-storage/pv-demo.yaml   # PV+PVC+消费 Pod
kubectl get pvc   # 实测: teleops-pvc Bound teleops-pv 1Gi RWO
kubectl exec -it pv-consumer -- ls /data   # 挂载验证
```

## 实验四：调度策略（模块 5，15 分钟）

```bash
kubectl label node teleops-control-plane node-type=core --overwrite
kubectl apply -f k8s/basics/05-scheduling/schedule-demo.yaml
kubectl get pod schedule-demo -o wide    # 实测: 调度到带标签节点
# 污点实验：kubectl taint node teleops-control-plane key=value:NoSchedule
#          → 无容忍的新 Pod Pending（观察 Events 里的 taint 原因）
```

## 实验五：配额与 QoS（模块 6，20 分钟）

```bash
kubectl apply -f k8s/basics/06-namespace-resources/quota-demo.yaml
kubectl get resourcequota rq -n quota-ns
# 实测: pods: 1/3  requests.cpu: 100m/500m  limits.memory: 64Mi/1Gi
kubectl get pod q1 -n quota-ns -o jsonpath='{.status.qosClass}'
# 实测: Guaranteed（requests==limits）
# 超配额实验：把副本扩到 4 → 第 4 个 Pod 被拒（exceeded quota 事件）
```

## 实验六：NetworkPolicy（模块 3，认知实验）

```bash
kubectl apply -f k8s/basics/03-networking/default-deny.yaml
kubectl get networkpolicy
# 注意：kindnet 不实现 NetworkPolicy（强制需 Calico/Cilium）——
# 这本身就是生产 CNI 选型的核心考题（文档 3 节）
```

## 清理

```bash
kubectl delete deployment web probe-demo; kubectl delete ns quota-ns
kind delete cluster --name teleops   # 全部实验结束后
```
