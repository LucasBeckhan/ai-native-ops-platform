// deepcopy.go：手写深拷贝（生产由 deepcopy-gen 生成，此处展示实现原理）
package v1

import "k8s.io/apimachinery/pkg/runtime"

func (in *NetworkServiceChain) DeepCopyInto(out *NetworkServiceChain) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	out.Spec.Functions = make([]ChainFunction, len(in.Spec.Functions))
	copy(out.Spec.Functions, in.Spec.Functions)
	out.Status = in.Status
}

func (in *NetworkServiceChain) DeepCopy() *NetworkServiceChain {
	if in == nil {
		return nil
	}
	out := new(NetworkServiceChain)
	in.DeepCopyInto(out)
	return out
}

func (in *NetworkServiceChain) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *NetworkServiceChainList) DeepCopyInto(out *NetworkServiceChainList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]NetworkServiceChain, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *NetworkServiceChainList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *NetworkServiceChainList) DeepCopy() *NetworkServiceChainList {
	if in == nil {
		return nil
	}
	out := new(NetworkServiceChainList)
	in.DeepCopyInto(out)
	return out
}
