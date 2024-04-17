package quman

import qmanservices "github.com/violetpay-org/point3-quman/services"

type Queue struct {
	QueueService          qmanservices.IQueueService
	LowLevelQueueOperator qmanservices.ILowLevelQueueOperator
	ConsumeCallback       qmanservices.QueueConsumeCallback
	StopCallback          qmanservices.QueueStopCallback
}

type QueueSet map[qmanservices.QueueName]*Queue
