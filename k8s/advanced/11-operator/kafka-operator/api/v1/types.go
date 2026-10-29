// =====================================================================
// 文件名: k8s/advanced/11-operator/kafka-operator/api/v1/types.go
// 所属模块: 模块 11 —— KafkaCluster CRD 类型
// 能力：① 集群规格（副本/镜像/存储）② 声明式 Topic ③ Status 上报
// =====================================================================
package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KafkaTopicSpec：声明式 Topic
type KafkaTopicSpec struct {
	Name              string            `json:"name"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replicationFactor"`
	Configs           map[string]string `json:"configs,omitempty"`
}

// KafkaClusterSpec：期望状态
type KafkaClusterSpec struct {
	Brokers   int32            `json:"brokers"`
	Image     string           `json:"image"`
	StorageGB int32            `json:"storageGB"`
	Topics    []KafkaTopicSpec `json:"topics,omitempty"`
}

// KafkaClusterStatus：实际状态
type KafkaClusterStatus struct {
	Phase        string `json:"phase"` // Provisioning / Ready / Degraded
	ReadyBrokers int32  `json:"readyBrokers"`
	Message      string `json:"message"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
//+kubebuilder:printcolumn:name="Brokers",type=integer,JSONPath=`.status.readyBrokers`
type KafkaCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              KafkaClusterSpec   `json:"spec,omitempty"`
	Status            KafkaClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true
type KafkaClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KafkaCluster `json:"items"`
}

func init() { SchemeBuilder.Register(&KafkaCluster{}, &KafkaClusterList{}) }
