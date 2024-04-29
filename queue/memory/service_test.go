package memoryqueue_test

import (
	"github.com/violetpay-org/queuemanager/config"
	"github.com/violetpay-org/queuemanager/queue"
	memoryqueue "github.com/violetpay-org/queuemanager/queue/memory"
	"testing"
)

func TestMemoryQueueService(t *testing.T) {
	testQueueName, err := config.RegisterQueueName("test_queue", 1)

	if err != nil {
		t.Error(err)
	}

	queueService, queueOperator := memoryqueue.NewMemoryQueueService(
		testQueueName,
		100,
	)

	if err != nil {
		t.Error(err)
	}

	t.Run(
		"Test memory queue service and operator",
		func(t *testing.T) { queue.ServiceTestSuite(t, queueService, queueOperator) },
	)
}
