package queueitem

import (
	"encoding/json"
)

type BaseQueueItem struct {
	OriginalQueueItem interface{}
}

func MakeQueueItem(item interface{}) Universal {
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

type QueueItemWithSerializer[T any] struct {
	OriginalQueueItem T
	Serializer[T]
}

type Serializer[T any] interface {
	FromJSON(jsonString string) (*T, error)
	ToJSON(item T) (string, error)
}

func MakeQueueItemWithSerializer[T any](item T, serializer Serializer[T]) Universal {
	return &QueueItemWithSerializer[T]{
		OriginalQueueItem: item,
		Serializer:        serializer,
	}
}

func (q *QueueItemWithSerializer[T]) QueueItemToJSON() (string, error) {
	queueJson, err := q.Serializer.ToJSON(q.OriginalQueueItem)

	if err != nil {
		return "", err
	}

	return queueJson, nil
}

func (q *QueueItemWithSerializer[T]) QueueItemToString() (string, error) {
	return q.QueueItemToJSON()
}
