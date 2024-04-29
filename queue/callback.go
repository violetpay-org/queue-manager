package queue

import "github.com/violetpay-org/queuemanager/item"

type ConsumeCallback interface {
	OnConsumed(item item.Universal)
	OnStop(int) // ?
}

type StopCallback interface {
	OnStop()
}
