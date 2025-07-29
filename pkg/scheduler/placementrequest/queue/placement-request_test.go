package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/api/scheduling/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPlacementRequestQueue(t *testing.T) {
	assert := assert.New(t)

	queue := NewPlacementRequestQueue()
	for i := range 10 {
		sub := time.Duration(i) * time.Hour * -1
		metav1.Now().Time.Add(-1 * time.Duration(i) * time.Hour)
		pr := &v1alpha2.PlacementRequest{
			ObjectMeta: metav1.ObjectMeta{
				CreationTimestamp: metav1.Time{
					Time: metav1.Now().Time.Add(sub),
				},
			},
		}
		queue.Push(pr)
	}

	var last *time.Time
	for range 10 {
		pr := queue.Pop()
		assert.NotNil(pr, "expected a placement request but got nil")

		current := &pr.CreationTimestamp.Time
		if last == nil {
			last = current
			continue
		}

		assert.Less(*last, *current, "expected placement request to be after the last one")
		last = current
	}
}

func TestPlacementRequestPushHandlers(t *testing.T) {
	assert := assert.New(t)

	counter := 0
	pushHandler := func() {
		counter++
	}

	queue := NewPlacementRequestQueue()
	queue.AddPushHandler(pushHandler)
	for range 10 {
		queue.Push(&v1alpha2.PlacementRequest{})
	}

	assert.Equal(10, counter, "expected push handler to be called 10 times")
}
