package innerqueue

import "github.com/violetpay-org/queue-manager/item"

type ConsumeCallback interface {
	OnConsumed(item queueitem.Universal)
	OnStop(int) // ?
}

type StopCallback interface {
	OnStop()
}
