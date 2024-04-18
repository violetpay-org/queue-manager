package redis

type Opts func(*Config)
type Config struct {
	QueueName string
	Retry     int
	TTL       int
	Addrs     []string
}

func SetQueueName(queueName string) Opts {
	return func(c *Config) {
		c.QueueName = queueName
	}
}

func AddBroker(broker string) Opts {
	return func(c *Config) {
		c.Addrs = append(c.Addrs, broker)
	}
}

func AddBrokers(brokers []string) Opts {
	return func(c *Config) {
		c.Addrs = append(c.Addrs, brokers...)
	}
}

func AddTTL(ttl int) Opts {
	return func(c *Config) {
		c.TTL = ttl
	}
}

func AddRetry(retry int) Opts {
	return func(c *Config) {
		c.Retry = retry
	}
}

func NewConfig(opts ...Opts) *Config {
	conf := Config{
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
