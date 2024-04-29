package config

import (
	"github.com/IBM/sarama"
	"time"
)

type RedisOpts func(*RedisConfig)
type RedisConfig struct {
	QueueName string
	Retry     int
	TTL       int
	Addrs     []string
}

func SetRedisQueueName(queueName string) RedisOpts {
	return func(c *RedisConfig) {
		c.QueueName = queueName
	}
}

func AddRedisBroker(broker string) RedisOpts {
	return func(c *RedisConfig) {
		c.Addrs = append(c.Addrs, broker)
	}
}

func AddRedisBrokers(brokers []string) RedisOpts {
	return func(c *RedisConfig) {
		c.Addrs = append(c.Addrs, brokers...)
	}
}

func AddRedisTTL(ttl int) RedisOpts {
	return func(c *RedisConfig) {
		c.TTL = ttl
	}
}

func AddRedisRetry(retry int) RedisOpts {
	return func(c *RedisConfig) {
		c.Retry = retry
	}
}

func NewRedisConfig(opts ...RedisOpts) *RedisConfig {
	conf := RedisConfig{
		QueueName: "",
		Retry:     0,
		TTL:       0,
		Addrs:     []string{},
	}

	for _, opt := range opts {
		opt(&conf)
	}

	return &conf
}

var (
	TestKafkaBrokers   = []string{}
	TestKafkaQueueName = "test_queue_2"
)

type KafkaOpts func(*KafkaConfig)
type KafkaConfig struct {
	Conf    *sarama.Config
	Brokers []string
	Topic   string
}

func AddKafkaBroker(broker string) KafkaOpts {
	return func(c *KafkaConfig) {
		c.Brokers = append(c.Brokers, broker)
	}
}

func AddKafkaBrokers(brokers []string) KafkaOpts {
	return func(c *KafkaConfig) {
		c.Brokers = append(c.Brokers, brokers...)
	}
}

func SetKafkaTopic(topic string) KafkaOpts {
	return func(c *KafkaConfig) {
		c.Topic = topic
	}
}

func NewKafkaConfig(opts ...KafkaOpts) *KafkaConfig {
	conf := KafkaConfig{
		Conf:    sarama.NewConfig(),
		Brokers: []string{"kafka.vp-datacenter-1.violetpay.net:9092", "kafka.vp-datacenter-1.violetpay.net:9093"},
	}

	for _, opt := range opts {
		opt(&conf)
	}

	conf.Conf.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	conf.Conf.Producer.Return.Successes = true
	conf.Conf.Producer.Flush.Frequency = 500 * time.Millisecond
	conf.Conf.Producer.RequiredAcks = sarama.WaitForAll

	// conf.Conf.Producer.Flush.Bytes = 1024 * 1024 * 10 // 10 MB
	return &conf
}
