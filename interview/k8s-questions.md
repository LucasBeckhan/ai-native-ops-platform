# K8s 方向面试题汇总

> 完整解析 12 道（k8s/basics/interview-basics.md，全部含考察意图/加分回答/误区）+ 本文件速答补充。

## 一、基础层完整解析索引（12 道）

apply 全流程 / etcd Raft / iptables vs IPVS / 三探针组合 / 滚动更新机制 / PV-PVC-SC / 调度预选优选 / Quota-LimitRange-QoS / OOMKilled 排查 / Pod IP 与网络链路 / Headless+StatefulSet / kind-minikube-kubeadm。

## 二、速答补充（20 道）

1. **Pause 容器的作用？** Pod 的 Infra 容器：持有网络/IPC 命名空间，业务容器重启不丢 IP。
2. **init 容器与 sidecar 的区别？** init 跑完即退（1.28+ 有原生 sidecar 容器：常驻且先启后停）。
3. **Probe 的 exec/httpGet/tcpSocket 怎么选？** 进程内语义用 http/tcp；启动前初始化检查用 exec（轻脚本）。
4. **静态 Pod 是什么？** kubelet 直接管（manifest 目录），控制面组件即静态 Pod（本集群实测）。
5. **kubectl apply 与 create 的区别？** apply 声明式可重复（last-applied 注解三方合并），create 命令式。
6. **Deployment 与 StatefulSet 的选择？** 无状态可互换副本用 Deployment；需要稳定标识+有序+每副本存储用 StatefulSet。
7. **DaemonSet 的典型用途？** 节点级代理：CNI/日志采集/监控 exporter（每节点一个）。
8. **Job 与 CronJob 的完成语义？** completions/parallelism/backoffLimit；CronJob 的 concurrencyPolicy（Allow/Forbid/Replace）。
9. **HPA 依赖什么组件？** Metrics Server（metrics.k8s.io API）；自定义指标走 prometheus-adapter/KEDA。
10. **PDB 的作用？** 自愿驱逐（drain/升级）时保底副本数——节点排水不中断服务的保障。
11. **kubelet 的优雅终止流程？** Pod Deleting → Endpoints 摘除（并行）→ preStop hook → SIGTERM → grace period 后 SIGKILL。
12. **Namespace 的资源隔离是硬隔离吗？** 默认只是逻辑隔离（对象边界）+Quota 限额；计算硬隔离靠节点池/虚拟化。
13. **configMap 热更新的生效条件？** 环境变量不更新（需重启）；volume 挂载自动更新（有同步延迟 ~1min）；应用需支持配置热载。
14. **Secret 的三种类型与加密现状？** Opaque/TLS/dockerconfigjson；etcd 中默认 base64 非 encryption at rest——生产配 KMS encryption provider。
15. **端口的 containerPort 会开放给外部吗？** 只是声明（信息性的）；实际暴露靠 Service/HostPort——安全组由 CNI/云实现。
16. **node 的 Internal-IP 与 Pod CIDR 的关系？** 节点 IP 是主机网络；Pod CIDR 是分配给节点的虚拟网段（CNI 规划，不重叠）。
17. **kubectl get 事件的保留期？** 默认 1 小时（event TTL）——排障要接事件流到外部存储。
18. **oom-score 由什么决定？** QoS（Guaranteed=-997）+ 内存用量——BestEffort 最先被节点 OOM killer 杀。
19. **如何限制某 ns 只能拉私有仓库镜像？** PSA + 准入策略（OPA/Gatekeeper 镜像前缀校验）。
20. **crd 与 apiservice 的区别？** CRD=etcd 存储的自定义资源；APIService=聚合到外部 apiserver（metrics-server 即此类）。
