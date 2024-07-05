package queueitem_test

import (
	"encoding/json"
	"github.com/violetpay-org/queue-manager/item"
	"testing"
)

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

// Test suite for queue item maker operations
func TestQueueItemMakerServiceTest(t *testing.T) {
	testItem := testObject{
		testField:  "test",
		testField2: 1,
		testNestedField: nestedTestObject{
			testField3: "nested",
		},
	}

	serializer := testObjectSerializer{} // Update the type to match the inferred type

	queueItem := queueitem.MakeQueueItemWithSerializer[testObject](testItem, &serializer)

	jsonMadeByQueueItem, err := queueItem.QueueItemToJSON()

	if err != nil {
		t.Errorf("Error converting item to JSON: %v", err)
	}

	jsonMadeBySerializer, err := serializer.ToJSON(testItem)

	if err != nil {
		t.Errorf("Error converting item to JSON: %v", err)
	}

	if jsonMadeByQueueItem != jsonMadeBySerializer {
		t.Errorf("JSONs are not equal")
	}
}
