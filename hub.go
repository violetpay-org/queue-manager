package queuemanager

import (
	queuemanagerconfig "github.com/violetpay-org/queue-manager/config"
	"github.com/violetpay-org/queue-manager/internal/queue/kafka"
	"github.com/violetpay-org/queue-manager/internal/queue/redis"
	queueitem "github.com/violetpay-org/queue-manager/item"
)

// NewRedisHub is a function that returns a new Hub.
func NewRedisHub(
	messageSerializer queueitem.RedisSerializer,
	config *queuemanagerconfig.RedisConfig,
	logger func(string),
) *redis.Hub {
	return redis.NewHub(
		messageSerializer,
		config,
		logger,
	)
}

// NewKafkaHub is a function that returns a new Hub.
// maxConsumerCount is the maximum number of consumers that can be created.
func NewKafkaHub(
	maxConsumerCount int,
	messageSerializer queueitem.KafkaSerializer,
	publishOnly bool,
	config *queuemanagerconfig.KafkaConfig,
	logger func(string),
) *kafka.Hub {
	return kafka.NewHub(
		maxConsumerCount,
		messageSerializer,
		publishOnly,
		config,
		logger,
	)
}
