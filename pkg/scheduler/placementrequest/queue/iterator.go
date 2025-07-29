package queue

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"

	"k8s.io/api/scheduling/v1alpha2"
)

// QueueConfig defines the configuration for a queue in the queue iterator. It
// includes the name of the queue, its weight, and the actual queue. The Weight
// determines how often the queue will be processed in each iteration and it is
// proportional to the sum of all weights provided for the QueueIterator.
type QueueConfig struct {
	Name   string
	Weight int
	Queue  *PlacementRequestQueue
}

// Validate checks the QueueConfig for correctness. We ensure that the queue
// has a name, its weight is greater than zero and that we have a valid pointer
// to a PlacementRequestQueue.
func (c *QueueConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("queue name cannot be empty")
	}
	if c.Weight <= 0 {
		return fmt.Errorf("queue weight must be greater than zero")
	}
	if c.Queue == nil {
		return fmt.Errorf("queue reference cannot be nil")
	}
	return nil
}

// QueueIterator is an entity that iterates over multiple queues popping
// PlacementRequests from them respecting their Weight.
type QueueIterator struct {
	Next    chan *v1alpha2.PlacementRequest
	mtx     sync.Mutex
	configs []QueueConfig
	resume  chan bool
}

// Resume ensures we have a resume signal ready to be intercepted by the Run()
// loop. This signal is used so inform the loop that new elements have been
// added to one of the queues. If the signal channel is full then we can move
// on as we already have a "resume" scheduled.
func (q *QueueIterator) Resume() {
	select {
	case q.resume <- true:
	default:
	}
}

// AddQueue adds a new queue to the QueueIterator.
func (q *QueueIterator) AddQueue(cfg QueueConfig) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("error adding queue: %w", err)
	}

	q.mtx.Lock()
	defer q.mtx.Unlock()

	cfg.Queue.AddPushHandler(q.Resume)
	q.configs = append(q.configs, cfg)
	return nil
}

// Run starts the queue iterator. We start with a list with all queues and we
// select one based on the queue weights. If the selected queue delivers us a
// message we send it to the Next channel and restart the loop. If the selected
// queue does not deliver us a message we exclude it from the list of repeat
// the process. We keep doing this until we either find a message in one of the
// queues or we found all queues to be empty. In the latter case we then wait
// for a resume signal to be sent by the PushHandler of one of the queues.
func (q *QueueIterator) Run(ctx context.Context) {
	defer close(q.Next)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var cfgs []QueueConfig

		q.mtx.Lock()
		cfgs = append(cfgs, q.configs...)
		q.mtx.Unlock()

		allempty, nrconfigs := true, len(cfgs)
		for range nrconfigs {
			if i, end := q.readOne(ctx, cfgs); !end {
				cfgs = slices.Delete(cfgs, i, i+1)
				continue
			}

			allempty = false
			break
		}

		if allempty {
			select {
			case <-ctx.Done():
			case <-q.resume:
			}
		}
	}
}

// readOne reads a single PlacementRequest from the queues based on their
// weights. It selects the next queue to process and attempts to pop a
// PlacementRequest from it. If a request is found this function returns
// true, otherwise it returns false. The index of the selected queue is
// also returned. If a message is found it is written to the Next channel
// so this function may block.
func (q *QueueIterator) readOne(ctx context.Context, configs []QueueConfig) (int, bool) {
	index := q.selectNextQueue(configs)
	if pr := configs[index].Queue.Pop(); pr != nil {
		select {
		case <-ctx.Done():
		case q.Next <- pr:
		}
		return index, true
	}
	return index, false
}

// selectNextQueue uses the weights to select the next queue to process. It
// returns the index of the selected queue based on the weights provided in
// the QueueConfig. The selection is done in a weighted random manner.
func (q *QueueIterator) selectNextQueue(configs []QueueConfig) int {
	var total int
	for _, config := range configs {
		total += config.Weight
	}

	// select a random number between 1 and total + 1. this random number
	// will fit somewhere in the sum of all individual weights.
	selected := rand.Intn(total) + 1

	// we keep summing the weights until we find the sum to be greater than
	// or equal to the selected number. this means that the queue at that
	// index is the one we should process next.
	var sum int
	for i, config := range configs {
		if sum += config.Weight; selected <= sum {
			return i
		}
	}

	// this should never happen as we always select a number between 1 and
	// total + 1, which is the sum of all weights.
	panic("no queue selected, this should never happen")
}

// NewQueueIterator creates a queue iterator based on the provided QueueConfig
// objects. This function registers a custom push handler for each queue so it
// is capable of resuming reading from queues.
func NewQueueIterator(configs ...QueueConfig) (*QueueIterator, error) {
	it := &QueueIterator{
		Next:    make(chan *v1alpha2.PlacementRequest),
		resume:  make(chan bool, 2),
		configs: configs,
	}

	for _, qcfg := range configs {
		if err := qcfg.Validate(); err != nil {
			return nil, fmt.Errorf("error creating iterator: %w", err)
		}
		qcfg.Queue.AddPushHandler(it.Resume)
	}

	return it, nil
}
