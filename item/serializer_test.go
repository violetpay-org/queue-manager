package queueitem_test

import (
	"github.com/IBM/sarama"
	"github.com/violetpay-org/queuemanager/config"
	"github.com/violetpay-org/queuemanager/item"
	"reflect"
	"testing"
	"time"
)

type testItem struct {
	Item string `json:"item"`
}

func TestOpenBankingKafkaMessageSerializer(t *testing.T) {
	serializer := queueitem.NewKafkaSerializer(reflect.TypeOf(testItem{}))
	queueItem := queueitem.MakeQueueItem(testItem{Item: "test"})
	testQueueName, err := queuemanagerconfig.RegisterQueueName("test", 0)

	if err != nil {
		t.Error(err)
	}

	t.Run(
		"TestQueueItemToProducerMessage",
		func(t *testing.T) {
			message, err := serializer.QueueItemToProducerMessage(
				queueItem,
				testQueueName.GetQueueName(),
			)

			if err != nil {
				t.Error(err)
			}

			if message.Topic != testQueueName.GetQueueName() {
				t.Errorf("Topic should be " + testQueueName.GetQueueName())
			}

			if message.Value == nil {
				t.Error("Message value should not be nil")
			}
		},
	)

	t.Run(
		"TestConsumerMessageToQueueItem",
		func(t *testing.T) {

			message, err := serializer.QueueItemToProducerMessage(
				queueItem,
				testQueueName.GetQueueName(),
			)

			messageBytes, _ := message.Value.Encode()

			if err != nil {
				t.Error(err)
			}

			consumerMessage := sarama.ConsumerMessage{
				Value:     messageBytes,
				Headers:   []*sarama.RecordHeader{},
				Timestamp: time.Now(),
				Topic:     testQueueName.GetQueueName(),
				Partition: 0,
			}

			queueItemSerialized, err := serializer.ConsumerMessageToQueueItem(
				&consumerMessage,
			)

			if err != nil {
				t.Error(err)
			}

			jsonString, _ := queueItemSerialized.QueueItemToJSON()
			originalJsonString, _ := queueItem.QueueItemToJSON()

			if jsonString != originalJsonString {
				t.Errorf("Queue item is not the same\nExpected: %s\nGot: %s", originalJsonString, jsonString)
			}
		},
	)
}
