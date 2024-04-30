package memory

import "github.com/violetpay-org/queuemanager/item"

type PubSubQueue struct {
	maxItems int
	storage  chan queueitem.Universal
}

func NewPubSubQueue(maxItems int) *PubSubQueue {
	return &PubSubQueue{
		maxItems: maxItems,
		storage:  make(chan queueitem.Universal, maxItems),
	}
}

func (q *PubSubQueue) PushToTheQueue(item queueitem.Universal) error {
	q.storage <- item
	return nil
}

func (q *PubSubQueue) GetStorage() chan queueitem.Universal {
	return q.storage
}
