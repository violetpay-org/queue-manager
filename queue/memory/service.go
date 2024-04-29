package memoryqueue

import (
	"context"
	"fmt"
	"github.com/violetpay-org/queuemanager/config"
	"github.com/violetpay-org/queuemanager/internal/queue/memory"
	"github.com/violetpay-org/queuemanager/item"
	"github.com/violetpay-org/queuemanager/queue"
	"sync"
)

// Service for test and TestQueueOperator
type Service struct {
	queueName config.QueueName
	isStopped chan bool
	queue     memory.PubSubQueue
}

func NewMemoryQueueService(
	queueName config.QueueName,
	maxItems int,
) (queue.Service, queue.ILowLevelQueueOperator) {
	queue := &Service{
		queueName: queueName,
		isStopped: make(chan bool),
		queue:     *memory.NewPubSubQueue(maxItems),
	}

	return queue, queue
}

func (s *Service) GetQueueName() config.QueueName {
	return s.queueName
}

func (s *Service) PushToTheQueue(
	item item.Universal,
) error {
	err := s.queue.PushToTheQueue(item)
	if err != nil {
		return err
	}

	return nil
}

func (s *Service) StopQueue(
	callback queue.StopCallback,
) error {
	fmt.Println(fmt.Sprintf("Stopping %s queue", s.GetQueueName()))
	s.isStopped <- true
	callback.OnStop()
	return nil
}

func (s *Service) StartQueue(
	onConsume queue.ConsumeCallback,
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

func (s *Service) InsertMessage(item item.Universal) error {
	return s.PushToTheQueue(item)
}
