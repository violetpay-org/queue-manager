package kafkaqueue_test

import (
	"github.com/violetpay-org/queue-manager/config"
	"github.com/violetpay-org/queue-manager/item"
	"github.com/violetpay-org/queue-manager/queue"
	kafkaqueue "github.com/violetpay-org/queue-manager/queue/kafka"
	"reflect"
	"testing"
)

type testItem struct {
	Item string `json:"item"`
}

func TestKafkaQueueService(t *testing.T) {
	testQueueName, err := queuemanagerconfig.RegisterQueueName("test_queue", 1)
	testBrokers := []string{"kafka.vp-datacenter-1.violetpay.net:9092", "kafka.vp-datacenter-1.violetpay.net:9093"}

	if err != nil {
		t.Error(err)
	}

	queueService, queueOperator, err := kafkaqueue.NewQueue(
		kafkaqueue.Args{
			NumOfPartitions: 1,
			Brokers:         testBrokers,
			Serializer:      queueitem.NewKafkaSerializer(reflect.TypeOf(testItem{})),
			QueueName:       testQueueName,
			Logger:          func(string) {},
		},
	)

	if err != nil {
		t.Error(err)
	}

	t.Run(
		"Test Kafka queue service and operator",
		func(t *testing.T) { innerqueue.ServiceTestSuite(t, queueService, queueOperator) },
	)
}
