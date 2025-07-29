package queue

import (
	"container/heap"
	"sync"

	"k8s.io/api/scheduling/v1alpha2"
)

// PrioritizedPlacementRequest wraps a PlacementRequest and provides a function
// to return its priority. We need this because the PriorityQueue operates on
// objects that implement the Prioritized interface.
type PrioritizedPlacementRequest struct {
	*v1alpha2.PlacementRequest
}

// Priority returns the priority of the PlacementRequest. This is used by the
// PriorityQueue to determine the order of items in the queue. We are using
// the CreationTimestamp of the PlacementRequest as the priority, which means
// that older requests have higher priority. The priority is expressed as an
// int64 value representing the Unix timestamp in nanoseconds.
func (p *PrioritizedPlacementRequest) Priority() int64 {
	return p.PlacementRequest.CreationTimestamp.UnixNano()
}

// This global variable ensure that PrioritizedPlacementRequest implements the
// Prioritized interface.
var _ Prioritized = &PrioritizedPlacementRequest{}

// PlacementRequestQueue is a prioritized queue for PlacementRequest objects.
// Uses may call Push to add a PlacementRequest to the queue and Pop to remove
// the highest priority PlacementRequest from the queue.
type PlacementRequestQueue struct {
	mtx          sync.Mutex
	queue        *PriorityQueue
	pushHandlers []func()
}

// Push adds a PlacementRequest to the queue. The PlacementRequest is wrapped
// in a PrioritizedPlacementRequest to provide the necessary priority func.
// Push handlers are called after the PlacementRequest is added to the queue.
func (q *PlacementRequestQueue) Push(pr *v1alpha2.PlacementRequest) {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	wrapped := &PrioritizedPlacementRequest{PlacementRequest: pr}
	heap.Push(q.queue, wrapped)
	for _, handler := range q.pushHandlers {
		handler()
	}
}

// Pop removes and returns the highest priority PlacementRequest from the
// queue. If the queue is empty this function will return nil.
func (q *PlacementRequestQueue) Pop() *v1alpha2.PlacementRequest {
	q.mtx.Lock()
	defer q.mtx.Unlock()

	if q.queue.Len() == 0 {
		return nil
	}
	result := heap.Pop(q.queue).(*PrioritizedPlacementRequest)
	return result.PlacementRequest
}

// AddPushHandler adds a handler that is called every time a PlacementRequest
// is added to this queue.
func (q *PlacementRequestQueue) AddPushHandler(handler func()) {
	q.mtx.Lock()
	defer q.mtx.Unlock()
	q.pushHandlers = append(q.pushHandlers, handler)
}

// NewPlacementRequestQueue creates a new PlacementRequestQueue.
func NewPlacementRequestQueue() *PlacementRequestQueue {
	return &PlacementRequestQueue{
		queue:        newPriorityQueue(),
		pushHandlers: []func(){},
	}
}
