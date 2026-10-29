# Lab：Ingress 与金丝雀灰度（kind 真实集群，已实测）

> **前置**：kind 集群 teleops；本地已缓存 ingress-nginx v1.11.3 镜像
> **预计耗时**：60 分钟 · 对应模块 2/3/13（Ingress 原理/路由/灰度）

## 实验一：部署精简版 ingress-nginx（15 分钟）

```bash
kind load docker-image registry.k8s.io/ingress-nginx/controller:v1.11.3 --name teleops  # 离线注入
kubectl apply -f ingress-nginx-kind.yaml
kubectl -n ingress-nginx rollout status deployment/ingress-nginx-controller
```

**实测踩坑记录（本 lab 的第一手教学价值）**：精简掉 admission webhook 时必须同时移除
`--validating-webhook-certificate` 参数——保留参数而无证书挂载，控制器报
`lstat /usr/local/certificates: no such file or directory` → liveness 失败 → 反复重启（exit 137）。

## 实验二：路径路由（10 分钟）

部署两个版本后端（web-v1/web-v2，ConfigMap 区分页面）+ Ingress：

```bash
curl -s -H "Host: demo.teleops.local" localhost:30080/     # → App v1 (stable)
curl -s -H "Host: demo.teleops.local" localhost:30080/v2/  # → App v2 (canary)
```

> kind 默认不映射 NodePort 到宿主机，实验用 `kubectl port-forward svc/ingress-nginx-controller 30080:80 -n ingress-nginx` 访问。

## 实验三：金丝雀按权重灰度（20 分钟，★ 本 lab 核心）

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: web-canary
  annotations:
    nginx.ingress.kubernetes.io/canary: "true"
    nginx.ingress.kubernetes.io/canary-weight: "10"
spec:   # host/paths 与主 Ingress 相同，backend 指向 v2
```

**实测采样 100 次**：

```
 91 v1
  9 v2      ← 10% 权重精确生效
```

**实测踩坑二（重要）**：金丝雀后端（v2-svc）**不能同时是主 Ingress 同一 location 的主上游**——
控制器日志给出明确警告：
`alternative upstream web-v2-svc in Ingress web-canary is primary upstream in Other Ingress`
解决：主 Ingress 的 `/` 路径只指向 v1，v2 仅经金丝雀承接。这是"金丝雀不生效"的第一高频原因。

## 实验四：基于 Header 的定向灰度（10 分钟，可选）

```yaml
nginx.ingress.kubernetes.io/canary-by-header: "X-Canary"
# 内部测试人员带 X-Canary: always → 100% v2；其余走权重
curl -s -H "Host: demo.teleops.local" -H "X-Canary: always" localhost:30080/
# 实测 → App v2 (canary)
```

## 清理

```bash
kubectl delete ing web-canary web-path-ingress
kubectl delete deployment web-v1 web-v2 probe-demo web --ignore-not-found
```
