/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduling

import (
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// Supported PodGroupPolicy kinds.
	PodGroupPolicyKindDefault PodGroupPolicyKind = "Default"
	PodGroupPolicyKindGang    PodGroupPolicyKind = "Gang"
)

type PodGroupPolicyKind string

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Workload is a top-level type that represents a collection of PodGroups.
type Workload struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   WorkloadSpec   `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	Status WorkloadStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

type WorkloadSpec struct {
	// ControllerRef points to the true workload, e.g. Deployment. It is
	// optional to set and is intended to make this mapping easier for
	// things like CLI tools. This field is immutable.
	ControllerRef *apiv1.ObjectReference `json:"controllerRef" protobuf:"bytes,1,opt,name=controllerRef"`

	// PodGroups is a list of groups of pods. Each group may request gang
	// scheduling.
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge,retainKeys
	// +listType=map
	// +listMapKey=name
	PodGroups []PodGroup `json:"podGroups" patchStrategy:"merge,retainKeys" patchMergeKey:"name" protobuf:"bytes,2,rep,name=podGroups"`
}

type PodGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []PodGroup `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// PodGroup is a group of pods that may contain multiple shapes (PodSets) and
// may contain multiple dense indexes (PodSubGroups) and which can optionally
// be replicated in a variable number of identical copies.
type PodGroup struct {
	Name *string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// Number of identical instances of PodGroup that are part of the
	// Workload. Defaults to 1.
	Replicas int32 `json:"replicas" protobuf:"varint,2,opt,name=replicas"`

	// Policy defines the configuration of the PodGroup to enable different
	// scheduling policies.
	Policy PodGroupPolicy `json:"policy" protobuf:"bytes,3,opt,name=policy"`
}

// PodGroupPolicy defines scheduling configuration of a PodGroup.
type PodGroupPolicy struct {
	// Kind indicates which of the other fields is non-empty. Required.
	// +unionDiscriminator
	Kind PodGroupPolicyKind `json:"kind" protobuf:"bytes,1,opt,name=kind,casttype=PodGroupPolicyKind"`

	// Default scheduling policy (default Kubernetes behavior).
	Default *DefaultSchedulingPolicy `json:"default" protobuf:"bytes,2,opt,name=default"`

	// Gang scheduling policy (all-or-nothing scheduling semantics).
	Gang *GangSchedulingPolicy `json:"gang" protobuf:"bytes,3,opt,name=gang"`
}

// GangSchedulingPolicy represents options for how gang scheduling of one
// PodGroup should be handled.
type GangSchedulingPolicy struct {
	MinCount *int32 `json:"minCount" protobuf:"varint,1,opt,name=minCount"`
}

// DefaultSchedulingPolicy represents default scheduling behavior.
// For now this is effectively just a marker type.
type DefaultSchedulingPolicy struct{}

type WorkloadStatus struct{}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type WorkloadList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Items []Workload `json:"items" protobuf:"bytes,2,rep,name=items"`
}
