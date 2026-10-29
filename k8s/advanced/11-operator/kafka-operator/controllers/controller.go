// =====================================================================
// 文件名: k8s/advanced/11-operator/kafka-operator/controllers/controller.go
// 所属模块: 模块 11 —— KafkaCluster Operator 的 Reconcile 逻辑
// 说明:
//   - Reconcile 三步（每步都体现 Operator 设计模式）：
//     ① Broker 收敛：StatefulSet（稳定网络标识 kafka-{i}， Broker 扩容=加副本）
//     ② Topic 收敛：声明式对齐（新建/扩分区；只增不减——Kafka 分区不可减）
//     ③ Status 上报：Provisioning/Ready/Degraded
//   - Finalizer：删除集群前记录 Broker 数据目录清单（人工归档提示）
//   - 与 Docker 集群的关系：本 Operator 管理的 Broker 以 StatefulSet 形式
//     运行（PVC 持久化 log.dirs），与课程的 docker-compose 集群是两种载体。
//   - 编译: GOPROXY=https://goproxy.cn,direct go build ./...
//   - envtest 单测骨架见 controllers/controller_test.go（追问第 1 轮要求）
// =====================================================================
package controllers

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	kafkav1 "teleops/kafka-operator/api/v1"
)

const kafkaFinalizer = "kafka.teleops.io/finalizer"

// KafkaClusterReconciler：Kafka 集群调和器
type KafkaClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=kafka.teleops.io,resources=kafkaclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;delete
//+kubebuilder:rbac:groups="",resources=services;configmaps,verbs=get;list;watch;create;update;delete

func (r *KafkaClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cluster kafkav1.KafkaCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// ---- Finalizer：删除前提示数据归档（真实场景：触发数据迁移 Job）----
	if !cluster.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&cluster, kafkaFinalizer) {
			fmt.Printf("[kafka-op] 集群 %s 删除前请确认 Broker 数据已归档（PVC 保留）\n", cluster.Name)
			controllerutil.RemoveFinalizer(&cluster, kafkaFinalizer)
			return ctrl.Result{}, r.Update(ctx, &cluster)
		}
		return ctrl.Result{}, nil
	}
	if !controllerutil.ContainsFinalizer(&cluster, kafkaFinalizer) {
		controllerutil.AddFinalizer(&cluster, kafkaFinalizer)
		if err := r.Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	// ---- 步骤①：Broker StatefulSet 收敛 ----
	if err := r.ensureBrokerStatefulSet(ctx, &cluster); err != nil {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	// ---- 步骤②：声明式 Topic 对齐 ----
	if err := r.reconcileTopics(ctx, &cluster); err != nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second},
			fmt.Errorf("topic 对齐失败（Kafka 未就绪或权限不足）: %w", err)
	}

	// ---- 步骤③：Status 上报 ----
	var sts appsv1.StatefulSet
	ready := int32(0)
	if err := r.Get(ctx, client.ObjectKeyFromObject(&cluster), &sts); err == nil {
		ready = sts.Status.ReadyReplicas
	}
	phase := "Provisioning"
	if ready == cluster.Spec.Brokers {
		phase = "Ready"
	} else if ready > 0 {
		phase = "Degraded"
	}
	cluster.Status.Phase = phase
	cluster.Status.ReadyBrokers = ready
	cluster.Status.Message = fmt.Sprintf("%d/%d broker 就绪", ready, cluster.Spec.Brokers)
	if err := r.Status().Update(ctx, &cluster); err != nil {
		return ctrl.Result{}, err
	}
	if phase != "Ready" {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	return ctrl.Result{}, nil
}

// ensureBrokerStatefulSet：Broker StatefulSet（KRaft 模式，combined 角色）。
// 稳定网络标识 kafka-{ordinal} 由 Headless Service 提供——Kafka 集群选型的核心理由。
func (r *KafkaClusterReconciler) ensureBrokerStatefulSet(
	ctx context.Context, cluster *kafkav1.KafkaCluster) error {

	headless := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: cluster.Name + "-headless", Namespace: cluster.Namespace},
		Spec: corev1.ServiceSpec{
			ClusterIP: "None",
			Selector:  map[string]string{"app": cluster.Name},
			Ports:     []corev1.ServicePort{{Name: "kafka", Port: 9092}},
		},
	}
	if err := controllerutil.SetControllerReference(cluster, headless, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, headless); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}

	// KRaft controller quorum voters：1..N @ kafka-{i}.headless:9093
	voters := ""
	for i := int32(0); i < cluster.Spec.Brokers; i++ {
		if i > 0 {
			voters += ","
		}
		voters += fmt.Sprintf("%d@%s-%d.%s-headless.%s.svc:9093",
			i+1, cluster.Name, i, cluster.Name, cluster.Namespace)
	}
	replicas := cluster.Spec.Brokers
	labels := map[string]string{"app": cluster.Name}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: cluster.Name, Namespace: cluster.Namespace},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: headless.Name,
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "kafka",
					Image: cluster.Spec.Image,
					Ports: []corev1.ContainerPort{
						{Name: "kafka", ContainerPort: 9092},
						{Name: "controller", ContainerPort: 9093},
					},
					Env: []corev1.EnvVar{
						{Name: "KAFKA_PROCESS_ROLES", Value: "broker,controller"},
						{Name: "KAFKA_CONTROLLER_QUORUM_VOTERS", Value: voters},
						{Name: "KAFKA_CONTROLLER_LISTENER_NAMES", Value: "CONTROLLER"},
						{Name: "KAFKA_LISTENERS", Value: "PLAINTEXT://:9092,CONTROLLER://:9093"},
						{Name: "KAFKA_NODE_ID", ValueFrom: &corev1.EnvVarSource{
							FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.labels['apps.kubernetes.io/pod-index']"}}},
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{
							Port: intstr.FromInt32(9092)}},
						InitialDelaySeconds: 20, PeriodSeconds: 10,
					},
				}}},
			},
			// PVC 模板：每 Broker 独立数据卷（StatefulSet 的存储语义）
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse(fmt.Sprintf("%dGi", cluster.Spec.StorageGB))},
					},
				},
			}},
		},
	}
	if err := controllerutil.SetControllerReference(cluster, sts, r.Scheme); err != nil {
		return err
	}
	err := r.Create(ctx, sts)
	if apierrors.IsAlreadyExists(err) {
		// 扩容收敛：副本数漂移时更新（缩容需人工确认——Kafka 缩 Broker 要迁移分区，
		// Operator 不自动缩，这是生产安全设计）
		var cur appsv1.StatefulSet
		if getErr := r.Get(ctx, client.ObjectKeyFromObject(sts), &cur); getErr == nil &&
			cur.Spec.Replicas != nil && *cur.Spec.Replicas < replicas {
			return r.Update(ctx, sts)
		}
		return nil
	}
	return err
}

// reconcileTopics：声明式 Topic 对齐。
// 教学实现说明：真实生产经 AdminClient API 对齐（扩分区/改配置）；
// 教学版把 Topic 清单写入 ConfigMap 供 Sidecar/Job 消费——关注"声明式对齐"
// 的模式而非 Kafka 协议细节。
func (r *KafkaClusterReconciler) reconcileTopics(ctx context.Context, cluster *kafkav1.KafkaCluster) error {
	if len(cluster.Spec.Topics) == 0 {
		return nil
	}
	desired := map[string]string{}
	for _, t := range cluster.Spec.Topics {
		desired[t.Name] = fmt.Sprintf("partitions=%d,rf=%d", t.Partitions, t.ReplicationFactor)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: cluster.Name + "-topics", Namespace: cluster.Namespace},
		Data:       desired,
	}
	if err := controllerutil.SetControllerReference(cluster, cm, r.Scheme); err != nil {
		return err
	}
	err := r.Create(ctx, cm)
	if apierrors.IsAlreadyExists(err) {
		var cur corev1.ConfigMap
		if getErr := r.Get(ctx, client.ObjectKeyFromObject(cm), &cur); getErr == nil {
			// 只增不减：声明里删掉的 Topic 不自动删除（Kafka 事故防线）
			for name, val := range cur.Data {
				if _, ok := desired[name]; !ok {
					desired[name] = val + "  # 已从声明移除，人工确认后删除"
				}
			}
			cm.Data = desired
			return r.Update(ctx, cm)
		}
	}
	return err
}

func (r *KafkaClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kafkav1.KafkaCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}
