// =====================================================================
// 文件名: k8s/advanced/11-operator/kafka-operator/api/v1/types.go
// 所属模块: 模块 11 —— KafkaCluster CRD 类型（管理 Kafka 集群的 Operator）
// 说明: 追问第 1 轮点名的完整实战。CRD 三大能力：
//   ① 集群规格（副本数/镜像/存储） ② Topic 管理（声明式 Topic 列表）
//   ③ Status 上报（阶段/就绪 Broker 数）
// =====================================================================
package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KafkaTopicSpec：声明式 Topic（Operator 对齐期望分区/副本/配置）
type KafkaTopicSpec struct {
	Name              string            `json:"name"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replicationFactor"`
	Configs           map[string]string `json:"configs,omitempty"` // min.insync.replicas 等
}

// KafkaClusterSpec：期望状态
type KafkaClusterSpec struct {
	Brokers    int32            `json:"brokers"`              // Broker 数
	Image      string           `json:"image"`                // Kafka 镜像
	StorageGB  int32            `json:"storageGB"`            // 每 Broker 存储大小
	Topics     []KafkaTopicSpec `json:"topics,omitempty"`     // 声明式 Topic 管理
	Resources  string           `json:"resources,omitempty"`  // 简化教学：资源需求 JSON 透传
}

// KafkaClusterStatus：实际状态
type KafkaClusterStatus struct {
	Phase        string  `json:"phase"`        // Provisioning / Ready / Degraded
	ReadyBrokers int32   `json:"readyBrokers"` // 就绪 Broker 数
	Message      string  `json:"message"`
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
