package queuemanager

import (
	"github.com/violetpay-org/queuemanager/config"
	queue2 "github.com/violetpay-org/queuemanager/queue"
)

type Queue struct {
	QueueService          queue2.Service
	LowLevelQueueOperator queue2.ILowLevelQueueOperator
	ConsumeCallback       queue2.ConsumeCallback
	StopCallback          queue2.StopCallback
}

type QueueSet map[config.QueueName]*Queue
