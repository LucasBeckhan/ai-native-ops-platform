// =====================================================================
// 文件名: telecom/nfv-orchestrator/operator/main.go
// 所属模块: 场景四 —— NFV 网络服务编排 Operator（K8s CRD 控制器）
// 说明:
//   - 场景四的核心：NetworkServiceChain CRD 的 Reconcile 循环——
//     声明"防火墙→IDS→负载均衡"服务链，控制器让实际拓扑收敛到声明。
//   - 模块 11（Operator 开发）在 NFV 场景的应用：
//     CRD（manifests/crd.yaml）+ controller-runtime + Finalizer + Status。
//   - 编译: cd telecom/nfv-orchestrator/operator && go mod tidy && go build
//     （依赖经 GOPROXY=https://goproxy.cn 下载；离线环境做代码走读）
// =====================================================================
package main

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	nfvv1 "teleops/nfv/api/v1"
)

// NetworkServiceChainReconciler：让集群实际状态收敛到 CR 声明
type NetworkServiceChainReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// Finalizer：删除 CR 前先摘除流量（防"删了 CR 流量黑洞"）
const nfvFinalizer = "nfv.teleops.io/finalizer"

//+kubebuilder:rbac:groups=nfv.teleops.io,resources=networkservicechains,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;delete

func (r *NetworkServiceChainReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var chain nfvv1.NetworkServiceChain
	if err := r.Get(ctx, req.NamespacedName, &chain); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err) // 已删除
	}

	// ---- Finalizer：删除前摘流 ----
	if !chain.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&chain, nfvFinalizer) {
			if err := r.drainTraffic(&chain); err != nil {
				return ctrl.Result{}, err // 摘流失败重试，不放行删除
			}
			controllerutil.RemoveFinalizer(&chain, nfvFinalizer)
			return ctrl.Result{}, r.Update(ctx, &chain)
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&chain, nfvFinalizer) {
		controllerutil.AddFinalizer(&chain, nfvFinalizer)
		if err := r.Update(ctx, &chain); err != nil {
			return ctrl.Result{}, err
		}
	}

	// ---- 逐跳收敛：按声明顺序为每跳创建 VNF Deployment（幂等）----
	ready := int32(0)
	for i, fn := range chain.Spec.Functions {
		if err := r.ensureFunctionDeployment(ctx, &chain, fn, i); err != nil {
			return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		}
		var deploy appsv1.Deployment
		deployName := fmt.Sprintf("%s-%s-%d", chain.Name, fn.Type, i)
		if err := r.Get(ctx, client.ObjectKey{Namespace: chain.Namespace, Name: deployName}, &deploy); err == nil {
			if deploy.Status.ReadyReplicas == *deploy.Spec.Replicas {
				ready++
			}
		}
	}

	// ---- Status：阶段与就绪跳数（kubectl get nsc 可见）----
	phase := "Provisioning"
	if int(ready) == len(chain.Spec.Functions) {
		phase = "Serving"
	}
	chain.Status.Phase = phase
	chain.Status.ReadyHops = ready
	chain.Status.Message = fmt.Sprintf("%d/%d 跳就绪", ready, len(chain.Spec.Functions))
	if err := r.Status().Update(ctx, &chain); err != nil {
		return ctrl.Result{}, err
	}
	if phase != "Serving" {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

// ensureFunctionDeployment：为第 i 跳创建 VNF Deployment（存在则幂等跳过）。
// 镜像按功能类型选择——真实环境对接 VNF 厂商镜像仓库。
func (r *NetworkServiceChainReconciler) ensureFunctionDeployment(
	ctx context.Context, chain *nfvv1.NetworkServiceChain,
	fn nfvv1.ChainFunction, index int) error {

	images := map[string]string{
		"firewall":     "teleops/vnf-firewall:1.0.0",
		"ids":          "teleops/vnf-ids:1.0.0",
		"loadbalancer": "teleops/vnf-lb:1.0.0",
	}
	image, ok := images[fn.Type]
	if !ok {
		return fmt.Errorf("未知 VNF 类型: %s", fn.Type)
	}
	deployName := fmt.Sprintf("%s-%s-%d", chain.Name, fn.Type, index)
	replicas := fn.Replicas
	if replicas == 0 {
		replicas = 1
	}
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: deployName, Namespace: chain.Namespace,
			Labels: map[string]string{"nfv.chain": chain.Name, "nfv.hop": fmt.Sprint(index)},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				"nfv.chain": chain.Name, "nfv.hop": fmt.Sprint(index)}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					"nfv.chain": chain.Name, "nfv.hop": fmt.Sprint(index),
					"nfv.type": fn.Type}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  fn.Type,
					Image: image,
					Ports: []corev1.ContainerPort{{ContainerPort: fn.Port}},
				}}},
			},
		},
	}
	// OwnerReference：CR 删除时级联清理（配合 Finalizer 先摘流再删）
	if err := controllerutil.SetControllerReference(chain, deploy, r.Scheme); err != nil {
		return err
	}
	err := r.Create(ctx, deploy)
	if apierrors.IsAlreadyExists(err) {
		return nil // 幂等：已存在则跳过（真实实现还应校准 spec 漂移）
	}
	return err
}

// drainTraffic：删除 CR 前把链上流量导流到旁路（生产调用 SDN 控制器 API）。
func (r *NetworkServiceChainReconciler) drainTraffic(chain *nfvv1.NetworkServiceChain) error {
	fmt.Printf("[NFV] 摘除服务链 %s/%s 的流量导流规则\n", chain.Namespace, chain.Name)
	return nil
}

func main() {
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: newScheme()})
	if err != nil {
		panic(err)
	}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&nfvv1.NetworkServiceChain{}).
		Complete(&NetworkServiceChainReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}); err != nil {
		panic(err)
	}
	fmt.Println("NFV Operator 启动")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		panic(err)
	}
}

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = nfvv1.AddToScheme(s)
	return s
}
