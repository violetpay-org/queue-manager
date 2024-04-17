package qmanservices

import (
	"encoding/json"

	qmanitem "github.com/violetpay-org/point3-quman/item"
)

type BaseQueueItem struct {
	OriginalQueueItem interface{}
}

func MakeQueueItem(item interface{}) qmanitem.IQueueItem {
	return &BaseQueueItem{
		OriginalQueueItem: item,
	}
}

func (b *BaseQueueItem) QueueItemToJSON() (string, error) {
	queueJson, err := json.Marshal(b.OriginalQueueItem)

	if err != nil {
		return "", err
	}

	return string(queueJson), nil
}

func (b *BaseQueueItem) QueueItemToString() (string, error) {
	return b.QueueItemToJSON()
}
