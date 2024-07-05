package innerqueue

import (
	"context"
	"encoding/json"
	"github.com/violetpay-org/queue-manager/item"
	"sync"
	"testing"
	"time"
)

// ServiceTestSuite for successful queue service operations
// This test suite checks whether an item can be passed to the queue service
// and whether this item can be retrieved from the queue service
func ServiceTestSuite(
	t *testing.T,
	queueService Service,
	queueOperator ILowLevelQueueOperator,
) {
	queueItem := queueitem.NewTestQueueItem()

	wg := sync.WaitGroup{}
	context := context.Background()
	callback := consumptionTestCallback{t: t, consumed: false}

	err := queueOperator.StartQueue(
		&callback,
		&wg,
		&context,
	)

	if err != nil {
		t.Errorf("Error starting the queue: %v", err)
	}

	err = queueService.PushToTheQueue(queueItem)

	if err != nil {
		t.Errorf("Error pushing an item to the queue: %v", err)
	}

	time.Sleep(1 * time.Second)

	// Checks if the item is consumed by the queue service
	if !callback.IsConsumed() {
		t.Errorf("Item not consumed")
	}

	err = queueOperator.StopQueue(&queueStopCallback{t: t, stopped: false})

	if err != nil {
		t.Errorf("Error stopping the queue: %v", err)
	}
}

type testObject struct {
	testField       string
	testField2      int
	testNestedField nestedTestObject
}

type nestedTestObject struct {
	testField3 string
}

type testObjectSerializer struct{}

func (s *testObjectSerializer) FromJSON(jsonString string) (*testObject, error) {
	test := testObject{}
	nest := nestedTestObject{}

	temp := &struct {
		TestField  string `json:"testField"`
		TestField2 int    `json:"testField2"`
		TestField3 string `json:"testNestedField"`
	}{}

	err := json.Unmarshal([]byte(jsonString), temp)

	if err != nil {
		return nil, err
	}

	test.testField = temp.TestField
	test.testField2 = temp.TestField2
	nest.testField3 = temp.TestField3
	test.testNestedField = nest
	return &test, nil
}

func (s *testObjectSerializer) ToJSON(item testObject) (string, error) {
	temp := &struct {
		TestField  string `json:"testField"`
		TestField2 int    `json:"testField2"`
		TestField3 string `json:"testNestedField"`
	}{
		TestField:  item.testField,
		TestField2: item.testField2,
		TestField3: item.testNestedField.testField3,
	}

	jsonString, err := json.Marshal(temp)

	if err != nil {
		return "", err
	}

	return string(jsonString), nil
}

type consumptionTestCallback struct {
	t        *testing.T
	consumed bool
}

func (c *consumptionTestCallback) OnConsumed(queueItem queueitem.Universal) {
	if queueItem == nil {
		c.t.Errorf("Expected an item, but received nil")
	}

	c.consumed = true
}

func (c *consumptionTestCallback) OnStop(consumerId int) {}

func (c *consumptionTestCallback) IsConsumed() bool {
	return c.consumed
}

func (c *consumptionTestCallback) Reset() {
	c.consumed = false
}

type queueStopCallback struct {
	t       *testing.T
	stopped bool
}

func (c *queueStopCallback) OnStop() {
	c.stopped = true
}
