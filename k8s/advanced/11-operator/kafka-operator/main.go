// =====================================================================
// 文件名: k8s/advanced/11-operator/kafka-operator/main.go
// 说明: Kafka Operator 入口
// =====================================================================
package main

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"

	kafkav1 "teleops/kafka-operator/api/v1"
	"teleops/kafka-operator/controllers"
)

func main() {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = kafkav1.AddToScheme(s)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{Scheme: s})
	if err != nil {
		panic(err)
	}
	if err := (&controllers.KafkaClusterReconciler{
		Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		panic(err)
	}
	fmt.Println("Kafka Operator 启动")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		panic(err)
	}
}
