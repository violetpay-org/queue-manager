package qmanservices

import (
	"context"
	"sync"

	qmanitem "github.com/violetpay-org/point3-quman/item"
)

type IQueueService interface {
	GetQueueName() QueueName

	PushToTheQueue(
		item qmanitem.IQueueItem,
	) error
}

// ILowLevelQueueOperator allows for some low-level interactions with the queue itself.
// Through this interface, it is possible to run or stop some crucial operations.
type ILowLevelQueueOperator interface {
	StopQueue(
		callback QueueStopCallback,
	) error

	StartQueue(
		onConsume QueueConsumeCallback,
		waitGroup *sync.WaitGroup,
		ctx *context.Context,
	) error

	// This behavior is repeated IQueueService.PushToTheQueue
	InsertMessage(item qmanitem.IQueueItem) error
}
