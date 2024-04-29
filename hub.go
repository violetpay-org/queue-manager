package queuemanager

import (
	"github.com/violetpay-org/queuemanager/config"
	"github.com/violetpay-org/queuemanager/internal/queue/kafka"
	"github.com/violetpay-org/queuemanager/internal/queue/redis"
	"github.com/violetpay-org/queuemanager/item"
)

// NewRedisHub is a function that returns a new Hub.
func NewRedisHub(
	messageSerializer item.RedisSerializer,
	config *config.RedisConfig,
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
	messageSerializer item.KafkaSerializer,
	publishOnly bool,
	config *config.KafkaConfig,
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
