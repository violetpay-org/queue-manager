package qmanservices_test

import (
	"testing"

	qmanservices "github.com/violetpay-org/point3-quman/services"
)

func TestQueueItemMakerService(t *testing.T) {
	qmanservices.QueueItemMakerServiceTest(t)
}
