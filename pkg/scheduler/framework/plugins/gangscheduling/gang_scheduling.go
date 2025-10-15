/*
Copyright 2020 The Kubernetes Authors.

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

package gangscheduling

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/scheduling/v1alpha1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	corev1listers "k8s.io/client-go/listers/core/v1"
	v1alpha1listers "k8s.io/client-go/listers/scheduling/v1alpha1"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"
)

var (
	_ framework.PreEnqueuePlugin  = &GangScheduling{}
	_ framework.EnqueueExtensions = &GangScheduling{}
	_ framework.PermitPlugin      = &GangScheduling{}
)

const (
	Name           = "GangScheduling"
	MaxWaitingTime = 30 * time.Minute
	LogLevelDebug  = 0
)

// GangScheduling is a plugin that schedules pods in a group.
type GangScheduling struct {
	logger    klog.Logger
	handle    framework.Handle
	aggrstore *PodAggregatedStore
	podlister corev1listers.PodLister
	wkllister v1alpha1listers.WorkloadLister
}

// New initializes and returns a new GangScheduling plugin.
func New(ctx context.Context, obj runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	return &GangScheduling{
		logger:    klog.FromContext(ctx),
		handle:    handle,
		aggrstore: NewPodAggregatedStore(ctx, handle),
		podlister: handle.SharedInformerFactory().Core().V1().Pods().Lister(),
		wkllister: handle.SharedInformerFactory().Scheduling().V1alpha1().Workloads().Lister(),
	}, nil
}

func (gs *GangScheduling) Name() string {
	return Name
}

// PreFilter checks if the given pod points to a workload and if it does so it
// makes sure it is only admitted if we already have the minimum number of pods
// required for the pod group. This is the first stage of the scheduling cycle
// so we need to hold the horses until we have enough pods to schedule.
func (gs *GangScheduling) PreEnqueue(ctx context.Context, pod *corev1.Pod) *framework.Status {
	logger := gs.logger.WithValues("step", "preenqueue", "pod", klog.KObj(pod))
	if !PodPointsToPodGroup(pod) {
		return framework.NewStatus(framework.Success, "")
	}

	// add the pod to our aggreated store so we can keep track of it.
	logger.V(LogLevelDebug).Info("processing pod")
	gs.aggrstore.Add(pod)

	wref := pod.Spec.Workload
	workload, err := gs.wkllister.Workloads(pod.Namespace).Get(wref.Name)
	if err != nil {
		if !kerrors.IsNotFound(err) {
			return framework.NewStatus(framework.Error, "fail to find workload")
		}
		logger.V(LogLevelDebug).Info("workload not found", "workload_ref", wref.Name)
		return framework.NewStatus(framework.Unschedulable, "workload not found")
	}

	// make sure we keep decorating the logger with useful info as we move
	// forward.
	logger = logger.WithValues("workload", klog.KObj(workload))

	// find the podgroup the pod belongs to.
	var podgroup *v1alpha1.PodGroup
	if podgroup = PodGroupForPod(workload, pod); podgroup == nil {
		logger.V(LogLevelDebug).Info("podgroup not found")
		return framework.NewStatus(framework.Unschedulable, "podgroup not found")
	}

	// if the podgroup does not use the gang scheduling policy it is safe
	// to just allow the pod to move forward. this plugin should only
	// interfere when the podgroup is using the gang scheduling policy.
	logger = logger.WithValues("podgroup", *podgroup.Name)
	if podgroup.Policy.Kind != v1alpha1.PodGroupPolicyKindGang {
		logger.V(LogLevelDebug).Info("podgroup without gang policy")
		return framework.NewStatus(framework.Success, "")
	}

	// if the workload does not specify a minimum count we assume it does
	// not care about it even though it uses the Gang policy. let's just
	// allow the pod to move forward.
	if podgroup.Policy.Gang.MinCount == nil {
		logger.V(LogLevelDebug).Info("podgroup without min count")
		return framework.NewStatus(framework.Success, "")
	}

	// if we don't have enough pods yet just return Unschedulable and wait
	// for more pods to show up.
	expected := *podgroup.Policy.Gang.MinCount
	actual := gs.aggrstore.CountPodsInPodGroup(pod)
	if actual < expected {
		logger.V(LogLevelDebug).Info("not enough pods", "expected", expected, "actual", actual)
		return framework.NewStatus(framework.Unschedulable, "not enough workload pods")
	}

	logger.V(LogLevelDebug).Info("pod enqueued", "expected", expected, "actual", actual)
	return framework.NewStatus(framework.Success, "")
}

// EventsToRegister purpose is to notify the scheduling framework about the
// events may change our scheduling decision. In our case every time a new
// pod is added or a workload is updated / added we want to move the pods
// we deemed unschedulable in the scheduling queue back to the active queue
// so that they can be re-evaluated.
func (gs *GangScheduling) EventsToRegister(ctx context.Context) ([]framework.ClusterEventWithHint, error) {
	resource := fmt.Sprintf(
		"workloads.%s.%s", v1alpha1.SchemeGroupVersion.Version, v1alpha1.SchemeGroupVersion.Group,
	)
	return []framework.ClusterEventWithHint{
		{
			Event: framework.ClusterEvent{
				Resource:   framework.Pod,
				ActionType: framework.Add | framework.Update,
			},
		},
		{
			Event: framework.ClusterEvent{
				Resource:   framework.EventResource(resource),
				ActionType: framework.Add | framework.Update,
			},
		},
	}, nil
}

// Permit keep pods on waiting until the whole pod group is ready to be
// scheduled. This is the last stage this plugins touches in the scheduling
// cycle.
func (gs *GangScheduling) Permit(ctx context.Context, state framework.CycleState, pod *corev1.Pod, node string) (*framework.Status, time.Duration) {
	logger := gs.logger.WithValues("step", "permit", "pod", klog.KObj(pod))
	if !PodPointsToPodGroup(pod) {
		return framework.NewStatus(framework.Success, ""), 0
	}

	wref := pod.Spec.Workload
	workload, err := gs.wkllister.Workloads(pod.Namespace).Get(wref.Name)
	if err != nil {
		logger.V(LogLevelDebug).Info("workload not found", "workload_ref", wref.Name)
		return framework.AsStatus(err), 0
	}

	var podgroup *v1alpha1.PodGroup
	if podgroup = PodGroupForPod(workload, pod); podgroup == nil {
		err := errors.New("podgroup not found inside workload")
		return framework.AsStatus(err), 0
	}

	// if the podgroup does not use the gang scheduling policy it is safe
	// to just allow the pod to move forward. this plugin should only
	// interfere when the podgroup is using the gang scheduling policy.
	logger = logger.WithValues("podgroup", *podgroup.Name)
	if podgroup.Policy.Kind != v1alpha1.PodGroupPolicyKindGang {
		logger.V(LogLevelDebug).Info("podgroup without gang policy")
		return framework.NewStatus(framework.Success, ""), 0
	}

	// if the workload does not specify a minimum count we assume it does
	// not care about it even though it uses the Gang policy. let's just
	// allow the pod to move forward.
	if podgroup.Policy.Gang.MinCount == nil {
		logger.V(LogLevelDebug).Info("podgroup without min count")
		return framework.NewStatus(framework.Success, ""), 0
	}

	// we get the identifier for the pod group and start the count at one
	// as we are currently processing a pod from this pod group. we then
	// iterate over all waiting pods in the scheduling queue and count how
	// many of them belong to the same pod group.
	lookfor, wpods := PodGroupIndexKey(pod), []framework.WaitingPod{}
	gs.handle.IterateOverWaitingPods(
		func(wpod framework.WaitingPod) {
			if PodGroupIndexKey(wpod.GetPod()) == lookfor {
				wpods = append(wpods, wpod)
			}
		},
	)

	// the actual number of pods waiting is computed by the sum of the pods
	// currently waiting plus the pod we are processing.
	actual := int32(len(wpods)) + 1
	logger.V(LogLevelDebug).Info("found waiting pods", "actual", actual)

	// verify if the amount of pods awaiting (plus the pod we are currently
	// assessing) is enough to satisfy the minimum count required by the
	// pod group. if not enough just return "Wait".
	if actual < *podgroup.Policy.Gang.MinCount {
		// before putting a pod to wait we need to activate all the
		// pods from the same pod group so the schedule process start
		// for them.
		pods, err := gs.aggrstore.PodsInPodGroup(pod)
		if err != nil {
			logger.Error(err, "cannot retrieve pods in workload")
			return framework.AsStatus(err), 0
		}
		gs.handle.Activate(gs.logger, pods)

		logger.V(LogLevelDebug).Info("adding pod to waiting")
		return framework.NewStatus(framework.Wait, ""), MaxWaitingTime
	}

	// if we get here then we have enough pods waiting to get the whole
	// gang scheduled. we can authorize the pod to be scheduled but we
	// also need to make sure all other waiting pods are also allowed to
	// be scheduled.
	logger.V(LogLevelDebug).Info("enough pods, allowing them")
	for _, wpod := range wpods {
		wpod.Allow(Name)
	}

	logger.V(LogLevelDebug).Info("allowing pod")
	return framework.NewStatus(framework.Success, ""), 0
}
