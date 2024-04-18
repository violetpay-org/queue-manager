package kafka

import (
	"time"

	"github.com/IBM/sarama"
)

var (
	TestBrokers   = []string{}
	TestQueueName = "test_queue"
)

type Opts func(*Config)
type Config struct {
	Conf    *sarama.Config
	Brokers []string
	Topic   string
}

func AddBroker(broker string) Opts {
	return func(c *Config) {
		c.Brokers = append(c.Brokers, broker)
	}
}

func AddBrokers(brokers []string) Opts {
	return func(c *Config) {
		c.Brokers = append(c.Brokers, brokers...)
	}
}

func SetTopic(topic string) Opts {
	return func(c *Config) {
		c.Topic = topic
	}
}

func NewConfig(opts ...Opts) *Config {
	conf := Config{
		Conf:    sarama.NewConfig(),
		Brokers: []string{},
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
