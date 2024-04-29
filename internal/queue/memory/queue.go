package memory

import "github.com/violetpay-org/queuemanager/item"

type PubSubQueue struct {
	maxItems int
	storage  chan item.Universal
}

func NewPubSubQueue(maxItems int) *PubSubQueue {
	return &PubSubQueue{
		maxItems: maxItems,
		storage:  make(chan item.Universal, maxItems),
	}
}

func (q *PubSubQueue) PushToTheQueue(item item.Universal) error {
	q.storage <- item
	return nil
}

func (q *PubSubQueue) GetStorage() chan item.Universal {
	return q.storage
}
