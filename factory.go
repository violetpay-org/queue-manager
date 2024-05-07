package queuemanager

import (
	"context"
	"github.com/violetpay-org/queuemanager/config"
	"github.com/violetpay-org/queuemanager/internal/queueerror"
	innerqueue "github.com/violetpay-org/queuemanager/queue"
	"sync"
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

func (f *MainQueueFactory) RegisterQueueName(name string, index int) (queuemanagerconfig.QueueName, error) {
	return queuemanagerconfig.RegisterQueueName(name, index)
}

func (f *MainQueueFactory) GetWaitGroup() (*sync.WaitGroup, error) {
	if f.wg == nil {
		return nil, queueerror.ErrQueueFactoryWaitGroupNil()
	}

	return f.wg, nil
}

func (f *MainQueueFactory) GetQueueService(queueName queuemanagerconfig.QueueName) (innerqueue.Service, error) {
	queue := f.queueSet[queueName]

	if queue == nil {
		return nil, queueerror.ErrQueueNotFound(queueName.GetQueueName())
	}

	return queue.QueueService, nil
}

func (f *MainQueueFactory) RunQueue(queueName queuemanagerconfig.QueueName) (*sync.WaitGroup, error) {
	if f.wg == nil {
		return nil, queueerror.ErrQueueFactoryWaitGroupNil()
	}

	queue := f.queueSet[queueName]

	if queue == nil {
		return f.wg, queueerror.ErrQueueNotFound(queueName.GetQueueName())
	}

	queueOperator := queue.LowLevelQueueOperator
	queueCallbacks := queue.ConsumeCallback

	if (queueOperator == nil) || (queueCallbacks == nil) {
		return f.wg, queueerror.ErrQueueNotPrepared(queueName.GetQueueName())
	}

	err := queueOperator.StartQueue(
		queueCallbacks,
		f.wg,
		f.ctx,
	)

	return f.wg, err
}

func (f *MainQueueFactory) StopQueue(queueName queuemanagerconfig.QueueName) (*sync.WaitGroup, error) {
	if f.wg == nil {
		return nil, queueerror.ErrQueueFactoryWaitGroupNil()
	}

	queue := f.queueSet[queueName]

	if queue == nil {
		return f.wg, queueerror.ErrQueueNotFound(queueName.GetQueueName())
	}

	queueOperator := queue.LowLevelQueueOperator
	queueCallbacks := queue.StopCallback

	if queueOperator == nil || queueCallbacks == nil {
		return f.wg, queueerror.ErrQueueNotPrepared(queueName.GetQueueName())
	}

	err := queueOperator.StopQueue(queueCallbacks)

	return f.wg, err
}

func (f *MainQueueFactory) AddQueue(queueName queuemanagerconfig.QueueName, queue *Queue) error {
	if f.queueSet[queueName] != nil {
		return queueerror.ErrDuplicateQueue()
	}

	f.queueSet[queueName] = queue
	return nil
}

func (f *MainQueueFactory) AddAllQueues(queueSet QueueSet) error {
	// Check for duplicates
	for queueName := range queueSet {
		if f.queueSet[queueName] != nil {
			return queueerror.ErrDuplicateQueue()
		}
	}

	// Add all queues
	for queueName, queue := range queueSet {
		f.AddQueue(queueName, queue)
	}

	return nil
}

func (f *MainQueueFactory) UpsertQueue(queueName queuemanagerconfig.QueueName, queue *Queue) error {

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
	queuesExecuted := []queuemanagerconfig.QueueName{}

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

		return f.wg, queueerror.ErrQueueNotPrepared(erroredQueue.(queuemanagerconfig.QueueName).GetQueueName())
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
