package kafqueue_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/IBM/sarama"
	kafqueue "github.com/violetpay-org/point3-quman/kafka.queue"
	qmanservices "github.com/violetpay-org/point3-quman/services"
)

type testItem struct {
	Item string `json:"item"`
}

func TestOpenBankingKafkaMessageSerializer(t *testing.T) {
	serializer := kafqueue.NewSerializer(reflect.TypeOf(testItem{}))
	queueItem := qmanservices.MakeQueueItem(testItem{Item: "test"})
	testQueueName, err := qmanservices.RegisterQueueName("test", 0)

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
