package qmanitem

import "encoding/json"

type testQueueEntity struct {
	Item string
}

func (t *testQueueEntity) QueueItemToString() (string, error) {
	return t.QueueItemToJSON()
}

func (t *testQueueEntity) QueueItemToJSON() (string, error) {
	queueJson, err := json.Marshal(t)

	if err != nil {
		return "", err
	}

	return string(queueJson), nil
}

func NewTestQueueItem() IQueueItem {
	entity := &testQueueEntity{
		Item: "test",
	}

	return entity
}
