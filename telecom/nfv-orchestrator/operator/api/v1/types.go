// =====================================================================
// 文件名: telecom/nfv-orchestrator/operator/api/v1/types.go
// 说明: NetworkServiceChain CRD 的 Go 类型（对应 manifests/crd.yaml 的 Schema）
// =====================================================================
package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ChainFunction：服务链上的一跳（一个 VNF/CNF）
type ChainFunction struct {
	Type     string `json:"type"`     // firewall / ids / loadbalancer
	Port     int32  `json:"port"`     // VNF 服务端口
	Replicas int32  `json:"replicas"` // 副本数（0=默认 1）
}

// NetworkServiceChainSpec：期望的服务链拓扑（顺序即流量路径）
type NetworkServiceChainSpec struct {
	Functions []ChainFunction `json:"functions"` // 顺序：流量依次经过
	Ingress   string          `json:"ingress"`   // 流量入口（分类器标识）
}

// NetworkServiceChainStatus：实际状态（kubectl get nsc 可见）
type NetworkServiceChainStatus struct {
	Phase     string `json:"phase"`      // Provisioning / Serving
	ReadyHops int32  `json:"readyHops"`  // 就绪跳数
	Message   string `json:"message"`    // 人类可读摘要
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
//+kubebuilder:printcolumn:name="ReadyHops",type=integer,JSONPath=`.status.readyHops`
type NetworkServiceChain struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              NetworkServiceChainSpec   `json:"spec,omitempty"`
	Status            NetworkServiceChainStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true
type NetworkServiceChainList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NetworkServiceChain `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NetworkServiceChain{}, &NetworkServiceChainList{})
}
