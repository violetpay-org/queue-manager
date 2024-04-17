package memqueue

import (
	"context"
	"fmt"
	"sync"

	qmanitem "github.com/violetpay-org/point3-quman/item"
	qmanservices "github.com/violetpay-org/point3-quman/services"
)

type memoryPubSubQueue struct {
	maxItems int
	storage  chan qmanitem.IQueueItem
}

// TestQueueService and TestQueueOperator
type testQueueService struct {
	queueName qmanservices.QueueName
	isStopped chan bool
	queue     memoryPubSubQueue
}

func NewMemoryQueueService(
	queueName qmanservices.QueueName,
	maxItems int,
) (qmanservices.IQueueService, qmanservices.ILowLevelQueueOperator) {
	queue := &testQueueService{
		queueName: queueName,
		isStopped: make(chan bool),
		queue: memoryPubSubQueue{
			maxItems: maxItems,
			storage:  make(chan qmanitem.IQueueItem, maxItems),
		},
	}

	return queue, queue
}

func (q *testQueueService) GetQueueName() qmanservices.QueueName {
	return q.queueName
}

func (q *testQueueService) PushToTheQueue(
	item qmanitem.IQueueItem,
) error {
	q.queue.storage <- item
	return nil
}

func (q *testQueueService) StopQueue(
	callback qmanservices.QueueStopCallback,
) error {
	fmt.Println(fmt.Sprintf("Stopping %s queue", q.GetQueueName()))
	q.isStopped <- true
	callback.OnStop()
	return nil
}

func (q *testQueueService) StartQueue(
	onConsume qmanservices.QueueConsumeCallback,
	waitGroup *sync.WaitGroup,
	ctx *context.Context,
) error {
	fmt.Println(fmt.Sprintf("Starting %s queue", q.GetQueueName()))
	go func() {
		for {
			select {
			case <-q.isStopped:
				return
			case item := <-q.queue.storage:
				onConsume.OnConsumed(item)
			}
		}
	}()

	return nil
}

func (q *testQueueService) InsertMessage(item qmanitem.IQueueItem) error {
	return q.PushToTheQueue(item)
}
