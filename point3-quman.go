package quman

import (
	"context"
	"sync"

	qmanErr "github.com/violetpay-org/point3-quman/errors"
	qmanservices "github.com/violetpay-org/point3-quman/services"
)

// Main package for Quman (Queue Manager) service
type MainQueueFactory struct {
	queueSet QueueSet
	wg       *sync.WaitGroup
	ctx      *context.Context
}

func NewQueueFactory(wg *sync.WaitGroup, ctx *context.Context) *MainQueueFactory {
	return &MainQueueFactory{
		queueSet: make(QueueSet),
		wg:       wg,
		ctx:      ctx,
	}
}

func (f *MainQueueFactory) RegisterQueueName(name string, index int) (qmanservices.QueueName, error) {
	return qmanservices.RegisterQueueName(name, index)
}

func (f *MainQueueFactory) GetWaitGroup() (*sync.WaitGroup, error) {
	if f.wg == nil {
		return nil, qmanErr.ErrQueueFactoryWaitGroupNil()
	}

	return f.wg, nil
}

func (f *MainQueueFactory) GetQueueService(queueName qmanservices.QueueName) (qmanservices.IQueueService, error) {
	queue := f.queueSet[queueName]

	if queue == nil {
		return nil, qmanErr.ErrQueueNotFound(queueName.GetQueueName())
	}

	return queue.QueueService, nil
}

func (f *MainQueueFactory) RunQueue(queueName qmanservices.QueueName) (*sync.WaitGroup, error) {
	if f.wg == nil {
		return nil, qmanErr.ErrQueueFactoryWaitGroupNil()
	}

	queue := f.queueSet[queueName]

	if queue == nil {
		return f.wg, qmanErr.ErrQueueNotFound(queueName.GetQueueName())
	}

	queueOperator := queue.LowLevelQueueOperator
	queueCallbacks := queue.ConsumeCallback

	if (queueOperator == nil) || (queueCallbacks == nil) {
		return f.wg, qmanErr.ErrQueueNotPrepared(queueName.GetQueueName())
	}

	err := queueOperator.StartQueue(
		queueCallbacks,
		f.wg,
		f.ctx,
	)

	return f.wg, err
}

func (f *MainQueueFactory) StopQueue(queueName qmanservices.QueueName) (*sync.WaitGroup, error) {
	if f.wg == nil {
		return nil, qmanErr.ErrQueueFactoryWaitGroupNil()
	}

	queue := f.queueSet[queueName]

	if queue == nil {
		return f.wg, qmanErr.ErrQueueNotFound(queueName.GetQueueName())
	}

	queueOperator := queue.LowLevelQueueOperator
	queueCallbacks := queue.StopCallback

	if queueOperator == nil || queueCallbacks == nil {
		return f.wg, qmanErr.ErrQueueNotPrepared(queueName.GetQueueName())
	}

	err := queueOperator.StopQueue(queueCallbacks)

	return f.wg, err
}

func (f *MainQueueFactory) AddQueue(queueName qmanservices.QueueName, queue *Queue) error {
	if f.queueSet[queueName] != nil {
		return qmanErr.ErrDuplicateQueue()
	}

	f.queueSet[queueName] = queue
	return nil
}

func (f *MainQueueFactory) AddAllQueues(queueSet QueueSet) error {
	// Check for duplicates
	for queueName := range queueSet {
		if f.queueSet[queueName] != nil {
			return qmanErr.ErrDuplicateQueue()
		}
	}

	// Add all queues
	for queueName, queue := range queueSet {
		f.AddQueue(queueName, queue)
	}

	return nil
}

func (f *MainQueueFactory) UpsertQueue(queueName qmanservices.QueueName, queue *Queue) error {

	_, err := f.GetQueueService(queueName)

	if err != nil {
		return f.AddQueue(queueName, queue)
	}

	_, err = f.StopQueue(queueName)

	if err != nil {
		return err
	}

	err = f.AddQueue(queueName, queue)

	if err != nil {
		return err
	}

	return nil
}

func (f *MainQueueFactory) RunAllQueues() (*sync.WaitGroup, error) {
	var erroredQueue interface{}
	queuesExecuted := []qmanservices.QueueName{}

	for queueName := range f.queueSet {
		_, err := f.RunQueue(queueName)

		if err != nil {
			erroredQueue = queueName
			break
		}

		queuesExecuted = append(queuesExecuted, queueName)
	}

	if erroredQueue != nil {
		for _, queueName := range queuesExecuted {
			_, _ = f.StopQueue(queueName)
		}

		return f.wg, qmanErr.ErrQueueNotPrepared(erroredQueue.(qmanservices.QueueName).GetQueueName())
	}

	return f.wg, nil
}

func (f *MainQueueFactory) StopAllQueues() (*sync.WaitGroup, error) {
	var erroredQueue interface{}
	var err error

	for queueName := range f.queueSet {
		_, err = f.StopQueue(queueName)

		if err != nil {
			erroredQueue = queueName
			break
		}
	}

	if erroredQueue != nil {
		return f.wg, err
	}

	return f.wg, nil
}
