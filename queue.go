package queuemanager

import (
	queuemanagerconfig "github.com/violetpay-org/queue-manager/config"
	innerqueue "github.com/violetpay-org/queue-manager/queue"
)

type Queue struct {
	QueueService          innerqueue.Service
	LowLevelQueueOperator innerqueue.ILowLevelQueueOperator
	ConsumeCallback       innerqueue.ConsumeCallback
	StopCallback          innerqueue.StopCallback
}

type QueueSet map[queuemanagerconfig.QueueName]*Queue
