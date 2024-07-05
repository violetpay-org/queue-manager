package memoryqueue_test

import (
	queuemanagerconfig "github.com/violetpay-org/queue-manager/config"
	innerqueue "github.com/violetpay-org/queue-manager/queue"
	memoryqueue "github.com/violetpay-org/queue-manager/queue/memory"
	"testing"
)

func TestMemoryQueueService(t *testing.T) {
	testQueueName, err := queuemanagerconfig.RegisterQueueName("test_queue", 1)

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
		func(t *testing.T) { innerqueue.ServiceTestSuite(t, queueService, queueOperator) },
	)
}
