/*
Copyright 2025 The Kubernetes Authors.

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
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kube-scheduler/framework"
)

// PodAggregatedStore is used to keep track of the data we need to assess
// whether we can schedule a pod or not. Pod data is inserted here by means
// of the Add() method while removal is executed by means of the informer's
// OnDelete event handler.
type PodAggregatedStore struct {
	handle framework.Handle
	mtx    sync.RWMutex
	store  map[string]sets.Set[string]
	lister corev1listers.PodLister
}

// OnDelete removes a pod from the internal store when it is deleted. This is a
// no-op if the pod does not point to a workload or if its not found.
func (a *PodAggregatedStore) OnDelete(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	pgroupkey := PodGroupIndexKey(pod)
	if pgroupkey == "" {
		return
	}

	podkey, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		return
	}

	a.mtx.Lock()
	defer a.mtx.Unlock()
	if assigned, ok := a.store[pgroupkey]; ok {
		assigned.Delete(podkey)
	}
}

// OnUpdate manages pod updates. As .spec.workload inside a pod is defined as
// an immutable field we do not care about any other kind of update.
func (a *PodAggregatedStore) OnUpdate(any, any) {}

// OnAdd is a no-op. If we add pods through the informer event handler we may
// start to compete with the scheduler event handler. i.e. it is quite hard to
// ensure that a pod is processed by the scheduler only after it has been
// added to the aggregated store.
func (a *PodAggregatedStore) OnAdd(any, bool) {}

// Add adds a pod to the internal store. Pods are indexed by their pod groups
// inside their workloads. If the pod does not point to a workload this is a
// no-op.
func (a *PodAggregatedStore) Add(pod *v1.Pod) {
	pgroupkey := PodGroupIndexKey(pod)
	if pgroupkey == "" {
		return
	}

	podkey, err := cache.MetaNamespaceKeyFunc(pod)
	if err != nil {
		return
	}

	a.mtx.Lock()
	defer a.mtx.Unlock()
	if assigned, ok := a.store[pgroupkey]; ok {
		assigned.Insert(podkey)
		return
	}
	a.store[pgroupkey] = sets.New(podkey)
}

// PodsInPodGroup returns the list of pods currently registered for the
// podgroup the provided pod belongs to. If the pod does not belong to a
// workload or its podgroup is not valid it returns an empty list.
func (a *PodAggregatedStore) PodsInPodGroup(srcpod *corev1.Pod) (map[string]*corev1.Pod, error) {
	result := map[string]*corev1.Pod{}

	pgroupkey := PodGroupIndexKey(srcpod)
	if pgroupkey == "" {
		return result, nil
	}

	a.mtx.RLock()
	defer a.mtx.RUnlock()

	pods, ok := a.store[pgroupkey]
	if !ok {
		return result, nil
	}

	for pkey := range pods {
		ns, name, err := cache.SplitMetaNamespaceKey(pkey)
		if err != nil {
			return nil, fmt.Errorf("invalid pod key %s: %w", pkey, err)
		}

		pod, err := a.lister.Pods(ns).Get(name)
		if err != nil {
			return nil, fmt.Errorf("retrieving pod from lister: %w", err)
		}

		result[string(pod.UID)] = pod
	}
	return result, nil
}

// CountPodsInWorkloaPodGroup returns the number of pods currently registered
// for the pod group the provided pod belongs to. If the pod does not belong to
// a pod group it returns 0.
func (a *PodAggregatedStore) CountPodsInPodGroup(pod *corev1.Pod) int32 {
	pgroupkey := PodGroupIndexKey(pod)
	if pgroupkey == "" {
		return 0
	}

	a.mtx.RLock()
	defer a.mtx.RUnlock()
	if assigned, ok := a.store[pgroupkey]; ok {
		return int32(assigned.Len())
	}
	return 0
}

// NewPodAggregatedStore is used to create an aggregated store that is hooked up
// to a pod informer got from the framework handle. Such aggregated store can
// then be used to keep track of pods x workload relationships.
func NewPodAggregatedStore(_ context.Context, handle framework.Handle) *PodAggregatedStore {
	pods := handle.SharedInformerFactory().Core().V1().Pods()
	aggregator := &PodAggregatedStore{
		handle: handle,
		store:  map[string]sets.Set[string]{},
		lister: pods.Lister(),
	}
	pods.Informer().AddEventHandler(aggregator)
	return aggregator
}
