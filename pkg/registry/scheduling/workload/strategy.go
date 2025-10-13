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

package workload

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/apis/scheduling/validation"
)

// workloadStrategy implements verification logic for Workload.
type workloadStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating Workload objects.
var Strategy = workloadStrategy{legacyscheme.Scheme, names.SimpleNameGenerator}

// NamespaceScoped returns false because all Workloades are global.
func (workloadStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate clears the status of a Workload before creation.
func (workloadStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	pc := obj.(*scheduling.Workload)
	pc.Generation = 1
}

// PrepareForUpdate clears fields that are not allowed to be set by end users on update.
func (workloadStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {}

// Validate validates a new Workload.
func (workloadStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	pc := obj.(*scheduling.Workload)
	return validation.ValidateWorkload(pc)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (workloadStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize normalizes the object after validation.
func (workloadStrategy) Canonicalize(obj runtime.Object) {}

// AllowCreateOnUpdate is false for Workload; this means POST is needed to create one.
func (workloadStrategy) AllowCreateOnUpdate() bool {
	return false
}

// ValidateUpdate is the default update validation for an end user.
func (workloadStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateWorkloadUpdate(obj.(*scheduling.Workload), old.(*scheduling.Workload))
}

// WarningsOnUpdate returns warnings for the given update.
func (workloadStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate is the default update policy for Workload objects.
func (workloadStrategy) AllowUnconditionalUpdate() bool {
	return true
}
