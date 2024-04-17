package qmanservices

import (
	"context"
	"sync"
	"testing"
	"time"

	qmanitem "github.com/violetpay-org/point3-quman/item"
)

// Test suite for successful queue service operations
// This test suite checks whether an item can be passed to the queue service
// and whether this item can be retrieved from the queue service
func QueueServiceTestSuite(
	t *testing.T,
	queueService IQueueService,
	queueOperator ILowLevelQueueOperator,
) {
	queueItem := qmanitem.NewTestQueueItem()

	wg := sync.WaitGroup{}
	context := context.Background()
	callback := consumptionTestCallback{t: t, consumed: false}

	err := queueOperator.StartQueue(
		&callback,
		&wg,
		&context,
	)

	if err != nil {
		t.Errorf("Error starting the queue: %v", err)
	}

	err = queueService.PushToTheQueue(queueItem)

	if err != nil {
		t.Errorf("Error pushing an item to the queue: %v", err)
	}

	time.Sleep(1 * time.Second)

	// Checks if the item is consumed by the queue service
	if !callback.IsConsumed() {
		t.Errorf("Item not consumed")
	}

	err = queueOperator.StopQueue(&queueStopCallback{t: t, stopped: false})

	if err != nil {
		t.Errorf("Error stopping the queue: %v", err)
	}
}

type consumptionTestCallback struct {
	t        *testing.T
	consumed bool
}

func (c *consumptionTestCallback) OnConsumed(queueItem qmanitem.IQueueItem) {
	if queueItem == nil {
		c.t.Errorf("Expected an item, but received nil")
	}

	c.consumed = true
}

func (c *consumptionTestCallback) OnStop(consumerId int) {}

func (c *consumptionTestCallback) IsConsumed() bool {
	return c.consumed
}

func (c *consumptionTestCallback) Reset() {
	c.consumed = false
}

type queueStopCallback struct {
	t       *testing.T
	stopped bool
}

func (c *queueStopCallback) OnStop() {
	c.stopped = true
}
