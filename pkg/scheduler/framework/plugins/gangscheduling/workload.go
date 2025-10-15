package gangscheduling

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/scheduling/v1alpha1"
)

// PodPointsToPodGroup returns true if the given pod points to a workload.
func PodPointsToPodGroup(pod *corev1.Pod) bool {
	wref := pod.Spec.Workload
	return wref != nil && wref.Name != "" && wref.PodGroup != ""
}

// PodGroupIndexKey is used to assess a unique key for a given pod group across
// the whole cluster. If the pod does not point to a workload it returns an
// empty string.
func PodGroupIndexKey(pod *corev1.Pod) string {
	if !PodPointsToPodGroup(pod) {
		return ""
	}
	wref := pod.Spec.Workload
	return fmt.Sprintf("%s/%s/%s", pod.Namespace, wref.Name, wref.PodGroup)
}

// PodGroupForPod searches the provided workload for the pod group pointed by
// the pod. If the pod does not use workload or if the pod group is not found
// it returns nil.
func PodGroupForPod(workload *v1alpha1.Workload, pod *corev1.Pod) *v1alpha1.PodGroup {
	if !PodPointsToPodGroup(pod) {
		return nil
	}
	wref := pod.Spec.Workload
	for _, podgroup := range workload.Spec.PodGroups {
		if podgroup.Name != nil && *podgroup.Name == wref.PodGroup {
			return &podgroup
		}
	}
	return nil
}
