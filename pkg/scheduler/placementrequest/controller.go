package placementrequest

import (
	"context"
	"fmt"

	schedulingv1alpha2 "k8s.io/api/scheduling/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	schedulingv1alpha2listers "k8s.io/client-go/listers/scheduling/v1alpha2"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/scheduler/placementrequest/queue"
)

// PlacementRequestController is a controller for handling PlacementRequests.
type PlacementRequestController struct {
	prLister  schedulingv1alpha2listers.PlacementRequestLister
	podLister corev1listers.PodLister
	client    clientset.Interface
	queues    map[string]*queue.PlacementRequestQueue
	iterator  *queue.QueueIterator
}

// Run reads PlacementRequsts (already sorted by priority and weigth) and calls
// ScheduleOne for each one of them. This is a blocking function that returns
// only when the provided context is done. XXX some more error handling is
// needed here.
func (prc *PlacementRequestController) Run(ctx context.Context) {
	go prc.iterator.Run(ctx)
	for {
		select {
		case pr := <-prc.iterator.Next:
			_ = prc.ScheduleOne(ctx, pr)
		case <-ctx.Done():
			return
		}
	}
}

// ScheduleOne is the function responsible for evaluating if a PlacementRequest
// is valid and then bind it to the nodes. This function also sets the status
// once it is finished.
func (prc *PlacementRequestController) ScheduleOne(
	ctx context.Context, pr *schedulingv1alpha2.PlacementRequest,
) error {
	pr.Status.Result = schedulingv1alpha2.PlacementRequestResultSuccess
	pr.Status.Message = "The request was successfully scheduled"

	updater := prc.client.SchedulingV1alpha2().PlacementRequests(pr.Namespace)
	if _, err := updater.Update(ctx, pr, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update placement request status: %w", err)
	}
	fmt.Println("Placement request scheduled successfully:", pr.Name)
	return nil
}

// AddEventHandlers is used to make sure the informers are pointing to the
// right event handlers here. We want to enqueue every new PlacementRequest
// into our internal queues.
func (prc *PlacementRequestController) AddEventHandlers(informers informers.SharedInformerFactory) error {
	if _, err := informers.Scheduling().V1alpha2().PlacementRequests().Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch obj.(type) {
				case *schedulingv1alpha2.PlacementRequest:
					return true
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc: prc.enqueue,
			},
		},
	); err != nil {
		return fmt.Errorf("failed to add placement request event handler: %w", err)
	}
	return nil
}

// enqueue is called when a PlacementRequest is created on the cluster. This
// function responsibility is to enqueue the respective PlacementRequest object
// into one of our internal queues. We have one internal queue per scheduler
// name. If a queue for the scheduler name does not exist, we create it
// automatically.
func (prc *PlacementRequestController) enqueue(obj interface{}) {
	pr, ok := obj.(*schedulingv1alpha2.PlacementRequest)
	if !ok || pr.Spec.SchedulerName == "" {
		return
	}

	// if we already have a queue for the scheduler name, we just push
	// the PlacementRequest into it. they are going to be sorted by their
	// priority.
	if queue, ok := prc.queues[pr.Spec.SchedulerName]; ok {
		queue.Push(pr)
		return
	}

	// at this point we do not have a queue for the scheduler name, so we
	// need to create one and enqueue the PlacementRequest. XXX Weight here
	// should be read from the configuration file. Also, some more logging
	// is needed here.
	config := queue.QueueConfig{
		Name:   pr.Spec.SchedulerName,
		Weight: 1,
		Queue:  queue.NewPlacementRequestQueue(),
	}

	if err := prc.iterator.AddQueue(config); err != nil {
		return
	}

	prc.queues[pr.Spec.SchedulerName] = config.Queue
	config.Queue.Push(pr)
}

// New returns a PlacementRequest controller.
func New(
	ctx context.Context, client clientset.Interface, informers informers.SharedInformerFactory, opts ...Option,
) (*PlacementRequestController, error) {
	options := defaultOptions
	for _, opt := range opts {
		opt(&options)
	}

	iterator, err := queue.NewQueueIterator()
	if err != nil {
		return nil, fmt.Errorf("failed to create internal queue iterator: %w", err)
	}

	controller := &PlacementRequestController{
		prLister:  informers.Scheduling().V1alpha2().PlacementRequests().Lister(),
		podLister: informers.Core().V1().Pods().Lister(),
		client:    client,
		queues:    map[string]*queue.PlacementRequestQueue{},
		iterator:  iterator,
	}

	if err := controller.AddEventHandlers(informers); err != nil {
		return nil, fmt.Errorf("failed to add event handlers: %w", err)
	}

	return controller, nil
}
