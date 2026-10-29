// groupversion_info.go：controller-runtime 标准 scheme 注册（生产由 kubebuilder 生成）
package v1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	GroupVersion = schema.GroupVersion{Group: "nfv.teleops.io", Version: "v1"}
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	AddToScheme = SchemeBuilder.AddToScheme
)
