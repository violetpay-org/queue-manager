package item

import (
	"encoding/json"
)

type TestQueueEntity struct {
	Item string
}

func (t *TestQueueEntity) QueueItemToString() (string, error) {
	return t.QueueItemToJSON()
}

func (t *TestQueueEntity) QueueItemToJSON() (string, error) {
	queueJson, err := json.Marshal(t)

	if err != nil {
		return "", err
	}

	return string(queueJson), nil
}

func NewTestQueueItem() *TestQueueEntity {
	entity := &TestQueueEntity{
		Item: "test",
	}

	return entity
}
