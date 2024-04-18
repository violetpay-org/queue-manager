package kafqueue_test

import (
	"reflect"
	"testing"

	kafqueue "github.com/violetpay-org/point3-quman/kafka.queue"
	qmanservices "github.com/violetpay-org/point3-quman/services"
)

func TestKafkaQueueService(t *testing.T) {
	testQueueName, err := qmanservices.RegisterQueueName("test_queue", 1)
	testBrokers := []string{"b-1.vpkafkacluster1.w56rpl.c3.kafka.ap-northeast-2.amazonaws.com:9092", "b-2.vpkafkacluster1.w56rpl.c3.kafka.ap-northeast-2.amazonaws.com:9092"}

	if err != nil {
		t.Error(err)
	}

	queueService, queueOperator, err := kafqueue.NewQueue(
		kafqueue.Args{
			NumOfPartitions: 1,
			Brokers:         testBrokers,
			Serializer:      kafqueue.NewSerializer(reflect.TypeOf(testItem{})),
			QueueName:       testQueueName,
			Logger:          func(string) {},
		},
	)

	if err != nil {
		t.Error(err)
	}

	t.Run(
		"Test Kafka queue service and operator",
		func(t *testing.T) { qmanservices.QueueServiceTestSuite(t, queueService, queueOperator) },
	)
}
