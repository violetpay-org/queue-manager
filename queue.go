package queuemanager

import (
	"github.com/violetpay-org/queuemanager/config"
	"github.com/violetpay-org/queuemanager/queue"
)

type Queue struct {
	QueueService          innerqueue.Service
	LowLevelQueueOperator innerqueue.ILowLevelQueueOperator
	ConsumeCallback       innerqueue.ConsumeCallback
	StopCallback          innerqueue.StopCallback
}

type QueueSet map[queuemanagerconfig.QueueName]*Queue
