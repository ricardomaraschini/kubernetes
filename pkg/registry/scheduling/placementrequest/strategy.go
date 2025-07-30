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

package placementrequest

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/scheduling"
)

// placementRequestStrategy implements verification logic for PlacementRequests.
type placementRequestStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating PlacementRequest objects.
var Strategy = placementRequestStrategy{legacyscheme.Scheme, names.SimpleNameGenerator}

// NamespaceScoped returns true because all PlacementRequest are not global.
func (placementRequestStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate clears the status of a PlacementRequest before creation.
func (placementRequestStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	pc := obj.(*scheduling.PlacementRequest)
	pc.Generation = 1
}

// PrepareForUpdate clears fields that are not allowed to be set by end users on update.
func (placementRequestStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {}

// Validate validates a new PlacementRequest.
func (placementRequestStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return field.ErrorList{}
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (placementRequestStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize normalizes the object after validation.
func (placementRequestStrategy) Canonicalize(obj runtime.Object) {}

// AllowCreateOnUpdate is false for PlacementRequest; this means POST is needed to create one.
func (placementRequestStrategy) AllowCreateOnUpdate() bool {
	return false
}

// ValidateUpdate is the default update validation for an end user.
func (placementRequestStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return field.ErrorList{}
}

// WarningsOnUpdate returns warnings for the given update.
func (placementRequestStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate is the default update policy for PlacementRequest objects.
func (placementRequestStrategy) AllowUnconditionalUpdate() bool {
	return true
}
