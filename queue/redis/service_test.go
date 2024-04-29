package redisqueue_test

import (
	"github.com/violetpay-org/queuemanager/config"
	"github.com/violetpay-org/queuemanager/item"
	"github.com/violetpay-org/queuemanager/queue"
	redisqueue "github.com/violetpay-org/queuemanager/queue/redis"
	"reflect"
	"testing"
)

type testItem struct {
	Item string `json:"item"`
}

func TestRedisQueueService(t *testing.T) {
	testQueueName, err := config.RegisterQueueName("test_queue", 1)
	testBrokers := []string{"redis.vp-datacenter-1.violetpay.net:6379"}

	if err != nil {
		t.Error(err)
	}

	queueService, queueOperator, err := redisqueue.NewQueue(
		redisqueue.Args{
			MessageSerializer: item.NewRedisSerializer(reflect.TypeOf(testItem{})),
			QueueName:         testQueueName,
			Logger:            func(string) {},
			Brokers:           testBrokers,
		},
	)

	if err != nil {
		t.Error(err)
	}

	t.Run(
		"Test Redis queue service and operator",
		func(t *testing.T) { queue.ServiceTestSuite(t, queueService, queueOperator) },
	)
}
