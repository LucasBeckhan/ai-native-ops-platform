# kind 集群（本课程实验环境）——已实测创建成功
kind create cluster --name teleops --image kindest/node:v1.31.4 --wait 90s
kubectl get nodes   # 预期: teleops-control-plane Ready control-plane
# 观察控制面组件（模块 1 的活教材）
kubectl get pods -n kube-system
