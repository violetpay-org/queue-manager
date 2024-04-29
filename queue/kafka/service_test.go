package kafkaqueue_test

import (
	"github.com/violetpay-org/queuemanager/config"
	"github.com/violetpay-org/queuemanager/item"
	"github.com/violetpay-org/queuemanager/queue"
	kafkaqueue "github.com/violetpay-org/queuemanager/queue/kafka"
	"reflect"
	"testing"
)

type testItem struct {
	Item string `json:"item"`
}

func TestKafkaQueueService(t *testing.T) {
	testQueueName, err := config.RegisterQueueName("test_queue", 1)
	testBrokers := []string{"kafka.vp-datacenter-1.violetpay.net:9092", "kafka.vp-datacenter-1.violetpay.net:9093"}

	if err != nil {
		t.Error(err)
	}

	queueService, queueOperator, err := kafkaqueue.NewQueue(
		kafkaqueue.Args{
			NumOfPartitions: 1,
			Brokers:         testBrokers,
			Serializer:      item.NewKafkaSerializer(reflect.TypeOf(testItem{})),
			QueueName:       testQueueName,
			Logger:          func(string) {},
		},
	)

	if err != nil {
		t.Error(err)
	}

	t.Run(
		"Test Kafka queue service and operator",
		func(t *testing.T) { queue.ServiceTestSuite(t, queueService, queueOperator) },
	)
}
