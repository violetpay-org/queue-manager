package qmanservices

import qmanitem "github.com/violetpay-org/point3-quman/item"

type QueueConsumeCallback interface {
	OnConsumed(item qmanitem.IQueueItem)
	OnStop(int) // ?
}

type QueueStopCallback interface {
	OnStop()
}
