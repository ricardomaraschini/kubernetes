package queue

import (
	"container/heap"
)

// Prioritized is an item that has a priority, expressed as an integer.
type Prioritized interface {
	Priority() int64
}

// This global variable is used to ensure that the PriorityQueue implements the
// heap.Interface. It is not used directly but serves as a compile-time check.
var _ heap.Interface = &PriorityQueue{}

// PriorityQueue is a queue that implements the heap interface. This
// function operates only on items that comply with the Prioritized
// interface. Attempting to push or pop a different type will result
// in a panic. This struct isn't thread-safe, any blocking should be
// done upstream. Functions here are not meant to be called directly
// but rather through the heap go package.
type PriorityQueue struct {
	items []Prioritized
}

// Len return the number of items in the queue.
func (q *PriorityQueue) Len() int {
	return len(q.items)
}

// Less function is used to compare two prioritized objects based on their
// priority. The lower the number the higher the priority.
func (q *PriorityQueue) Less(i, j int) bool {
	return q.items[i].Priority() < q.items[j].Priority()
}

// Swap is used by the heap implementation to swap two elements in the queue.
func (q *PriorityQueue) Swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
}

// Push adds a new prioritized object onto the queue.
func (q *PriorityQueue) Push(pr any) {
	prioritized, ok := pr.(Prioritized)
	if !ok {
		panic("queue only accepts prioritized items")
	}
	q.items = append(q.items, prioritized)
}

// Pop removes and returns the highest priority placement request from the
// queue.
func (q *PriorityQueue) Pop() any {
	before, num := q.items, len(q.items)
	pr := before[num-1]
	q.items = before[0 : num-1]
	return pr
}

// newPriorityQueue initializes a new PriorityQueue and returns a pointer to
// it. The queue is initialized as an empty heap. We do not want to make it
// public as it is not meant to be used directly.
func newPriorityQueue() *PriorityQueue {
	q := &PriorityQueue{}
	heap.Init(q)
	return q
}
