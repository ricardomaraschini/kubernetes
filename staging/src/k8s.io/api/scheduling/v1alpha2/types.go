/*
Copyright 2022 The Kubernetes Authors.

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

package v1alpha2

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// PlacementRequestPolicyAllOrNothing indicates that either all
	// bindings in a placement request succeed or none of them must happen.
	PlacementRequestPolicyAllOrNothing PlacementRequestPolicy = "AllOrNothing"

	// PlacementRequestPolicyLenient indicates that it is ok if only a few
	// of the bindings suceeded inside a placement request.
	PlacementRequestPolicyLenient PlacementRequestPolicy = "Lenient"
)

const (
	// PlacementRequestResultUnknown indicates that the result of the
	// placement request is unknown, this is the default value.
	PlacementRequestResultUnknown PlacementRequestResult = ""

	// PlacementRequestResultSuccess indicates that the placement request
	// was processed successfully, this means that all bindings in the
	// request were processed successfully.
	PlacementRequestResultSuccess PlacementRequestResult = "Success"

	// PlacementRequestResultFailure indicates that the placement request
	// was not processed successfully.
	PlacementRequestResultFailure PlacementRequestResult = "Failure"

	// PlacementRequestResultRejected indicates that the placement request
	// was rejected, this means that the request was not processed and may
	// be invalid or not applicable.
	PlacementRequestResultRejected PlacementRequestResult = "Rejected"
)

// PlacementRequestPriority represents the priority for a given placement
// request. The higher this value the higher the priority.
type PlacementRequestPriority int

// PlacementRequestPolicy governs how the bindings in a placement
// request should be treated.
type PlacementRequestPolicy string

// PlacementRequestResult represents the outcome of the processing of
// a given placement request.
type PlacementRequestResult string

// PlacementRequestSpec holds the desired state for a placement request,
// indicating its policy and also a group of bindings.
type PlacementRequestSpec struct {
	// Policy indicates the relationship between the bindings in a
	// placement request.
	Policy PlacementRequestPolicy `json:"policy" protobuf:"bytes,1,opt,name=policy,casttype=PlacementRequestPolicy"`

	// Priority is an arbitrary integer, placement requests with a higher
	// priority are served first when processing the scheduler queue.
	Priority PlacementRequestPriority `json:"priority" protobuf:"varint,2,opt,name=priority,casttype=PlacementRequestPriority"`

	// SchedulerName is the name of the scheduler that is responsible for
	// creating this placement request. This is used to identify the
	// queue that the placement request belongs to.
	SchedulerName string `json:"schedulerName,omitempty" protobuf:"bytes,3,opt,name=schedulerName"`

	// Bingings is a list of bindings that the scheduler wants to have
	// processed by the placement request controller. Each binding contains
	// a pod and a node name, the controller will try to bind the pod to
	// the node.
	//
	// +listType=atomic
	Bindings []corev1.Binding `json:"bindings" protobuf:"bytes,4,rep,name=bindings"`
}

// PlacementRequestStatus holds the status for a PlacementRequest, it contains
// the current phase of the binding process, the actual result once the binding
// has finished, a reason and, in case of failure a user readable message. It
// also contains a list of individual bindings that correspond to the list
// provided on the spec, each individual binding may contain its own result.
type PlacementRequestStatus struct {
	// Result indicates the overall result of the placement request.
	Result PlacementRequestResult `json:"result" protobuf:"bytes,1,opt,name=result,casttype=PlacementRequestResult"`

	// Reason is a short, machine-readable string indicating the reason
	// for the result of the placement request.
	Reason string `json:"reason,omitempty" protobuf:"bytes,2,opt,name=reason"`

	// Message is a human-readable message indicating the reason for the
	// result of the placement request. This is intended to be used for
	// debugging purposes and should not be used for machine processing.
	Message string `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`

	// Bindings is a list of individual binding results that correspond
	// to the bindings provided in the spec. Each binding result contains
	// the result of the binding, a reason and a message. The ObjectMeta
	// of each binding result is expected to match the ObjectMeta of the
	// corresponding binding in the spec.
	//
	// +listType=atomic
	Bindings []PlacementRequestBindingResult `json:"bindings,omitempty" protobuf:"bytes,4,rep,name=bindings"`
}

// PlacementRequestBindingResult holds the result of a single binding
// inside a PlacementRequest object. Each binding in the spec is represented
// by a result and the ObjectMeta is expected to match.
type PlacementRequestBindingResult struct {
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Result  PlacementRequestResult `json:"result" protobuf:"bytes,2,opt,name=result,casttype=PlacementRequestResult"`
	Reason  string                 `json:"reason,omitempty" protobuf:"bytes,3,opt,name=reason"`
	Message string                 `json:"message,omitempty" protobuf:"bytes,4,opt,name=message"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PlacementRequest is a pod placement request sent to the placement request
// controller by a scheduler.
type PlacementRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Spec   PlacementRequestSpec   `json:"spec" protobuf:"bytes,2,opt,name=spec"`
	Status PlacementRequestStatus `json:"status" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PlacementRequestList is a collection of placement requests.
type PlacementRequestList struct {
	metav1.TypeMeta `json:",inline"`

	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// List of placement requests.
	// More info: https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller
	Items []PlacementRequest `json:"items" protobuf:"bytes,2,rep,name=items"`
}
