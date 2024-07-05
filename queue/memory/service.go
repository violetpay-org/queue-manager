package memoryqueue

import (
	"context"
	"fmt"
	"github.com/violetpay-org/queue-manager/config"
	"github.com/violetpay-org/queue-manager/internal/queue/memory"
	"github.com/violetpay-org/queue-manager/item"
	"github.com/violetpay-org/queue-manager/queue"
	"sync"
)

// Service for test and TestQueueOperator
type Service struct {
	queueName queuemanagerconfig.QueueName
	isStopped chan bool
	queue     memory.PubSubQueue
}

func NewMemoryQueueService(
	queueName queuemanagerconfig.QueueName,
	maxItems int,
) (innerqueue.Service, innerqueue.ILowLevelQueueOperator) {
	queue := &Service{
		queueName: queueName,
		isStopped: make(chan bool),
		queue:     *memory.NewPubSubQueue(maxItems),
	}

	return queue, queue
}

func (s *Service) GetQueueName() queuemanagerconfig.QueueName {
	return s.queueName
}

func (s *Service) PushToTheQueue(
	item queueitem.Universal,
) error {
	err := s.queue.PushToTheQueue(item)
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) StopQueue(
	callback innerqueue.StopCallback,
) error {
	fmt.Println(fmt.Sprintf("Stopping %s queue", s.GetQueueName()))
	s.isStopped <- true
	callback.OnStop()
	return nil
}

func (s *Service) StartQueue(
	onConsume innerqueue.ConsumeCallback,
	waitGroup *sync.WaitGroup,
	ctx *context.Context,
) error {
	fmt.Println(fmt.Sprintf("Starting %s queue", s.GetQueueName()))
	go func() {
		for {
			select {
			case <-s.isStopped:
				return
			case item := <-s.queue.GetStorage():
				onConsume.OnConsumed(item)
			}
		}
	}()

	return nil
}

func (s *Service) InsertMessage(item queueitem.Universal) error {
	return s.PushToTheQueue(item)
}
