package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	qmanitem "github.com/violetpay-org/point3-quman/item"
	qmanservices "github.com/violetpay-org/point3-quman/services"
)

// This Consumer is used to consume "Purchase" messages from Kafka.
// If you want to consume other messages, you should write another Consumer.
type Consumer struct {
	saramaConsumer    *sarama.PartitionConsumer
	saramaProducer    sarama.AsyncProducer
	connection        sarama.Client
	id                int
	idleIdChan        chan int
	messageSerializer KafkaMessageSerializer
	conf              *Config
	logger            func(string)

	// Status
	paused   chan bool
	isClosed bool
}

func NewConsumer(
	availIdChan chan int,
	messageSerializer KafkaMessageSerializer,
	conf *Config,
) (consumer *Consumer) {
	consumer = &Consumer{}
	consumer.messageSerializer = messageSerializer
	consumer.conf = conf
	consumer.init(availIdChan)
	return
}

func (c *Consumer) init(availIdChan chan int) {
	conn, err := sarama.NewClient(c.conf.Brokers, c.conf.Conf)
	if err != nil {
		panic(err)
	}

	c.idleIdChan = availIdChan

	c.connection = conn
	consumer, createConsumerErr := sarama.NewConsumerFromClient(conn)
	producer, createProducerErr := sarama.NewAsyncProducerFromClient(conn)

	if (createConsumerErr != nil) || (createProducerErr != nil) {
		panic(err)
	}

	// Wait for available consumer id
	c.id = <-c.idleIdChan

	consumePartition, err := consumer.ConsumePartition(
		c.conf.Topic,
		int32(c.id),
		sarama.OffsetNewest,
	)

	if err != nil {
		c.idleIdChan <- c.id
		panic(err)
	}

	c.saramaConsumer = &consumePartition
	c.saramaProducer = producer
	c.paused = make(chan bool)
	c.isClosed = false
	c.logger = func(string) {
		fmt.Println("[ERROR] Consumer", c.id, "error")
	}
}

func (c *Consumer) SetLogger(logger func(string)) {
	c.logger = logger
}

func (c *Consumer) GetConsumer() *sarama.PartitionConsumer {
	if c.saramaConsumer == nil {
		panic("Consumer is not initialized")
	}

	if c.isClosed {
		panic("Consumer is closed")
	}

	return c.saramaConsumer
}

func (c *Consumer) GetConnection() sarama.Client {
	if c.saramaConsumer == nil {
		panic("Consumer is not initialized")
	}

	if c.isClosed {
		panic("Consumer is closed")
	}

	return c.connection
}

func (c *Consumer) GetProducer() sarama.AsyncProducer {
	if c.saramaConsumer == nil {
		panic("Consumer is not initialized")
	}

	if c.isClosed {
		panic("Consumer is closed")
	}

	return c.saramaProducer
}

// SendMessage is a function that sends a message to Kafka.
// This function is used when the message itself is important.
// Use this function when you need to know if the message is sent.
func (c *Consumer) SendMessage(item qmanitem.IQueueItem, Topic string) error {
	mutex := sync.Mutex{}

	defer mutex.Unlock()

	mutex.Lock()
	producer := c.GetProducer()
	message, err := c.messageSerializer.QueueItemToProducerMessage(item, Topic)

	if err != nil {
		c.logger(err.Error())
		return err
	}

	producer.Input() <- message

	for {
		select {
		case <-producer.Successes():
			return nil
		case <-producer.Errors():
			return ErrProducingMessage
		}
	}
}

// FastSendMessage is a function that sends a message to Kafka without waiting for the result.
// This function is used when the message itself is not important. (e.g. logging, testing...)
// Use with caution because this function does not guarantee that the message is sent.
func (c *Consumer) FastSendMessage(item qmanitem.IQueueItem, Topic string) error {
	producer := c.GetProducer()
	message, err := c.messageSerializer.QueueItemToProducerMessage(item, Topic)

	if err != nil {
		return err
	}

	producer.Input() <- message

	return nil
}

// StartConsume is a function that starts consuming messages from Kafka.
// next is a callback function that is called when a message is consumed.
// callback is a callback function that is called when the consumer is stopped.
func (c *Consumer) StartConsume(
	callback qmanservices.QueueConsumeCallback,
	waitGroup *sync.WaitGroup,
	context *context.Context,
) (_, consumeError error) {
	if c.saramaConsumer == nil {
		panic("Consumer is not initialized")
	}

	if c.isClosed {
		panic("Consumer is closed")
	}

	defer waitGroup.Done()      // to notify the hub that the consumer is stopped
	defer callback.OnStop(c.id) // after the consumer is stopped
	defer c.stop()              // when the consumer is stopped

	fmt.Println("[INFO] Consumer", c.id, "started")

	ctx := *context
	saramaConsumer := *c.saramaConsumer

	for {
		select {
		/*
			case <-timeOut:
				go c.Pause()
				consumeError = fmt.Errorf("Consumer %d is paused because of timeout", c.id)
		*/
		case <-ctx.Done():
			go c.Pause()
		case err := <-saramaConsumer.Errors():
			// When an error occurs
			go c.Pause()
			consumeError = err
		case msg := <-saramaConsumer.Messages():
			// When received a message, call the callback function
			serializedMessage, err := c.messageSerializer.ConsumerMessageToQueueItem(msg)

			if err != nil {
				c.logger(err.Error())
			}

			// Consumes the serialized message
			go callback.OnConsumed(serializedMessage)
		case <-c.paused:
			// When the consumer is paused, close saramaConsumer
			c.close()
			close(c.paused)
			return nil, consumeError
		}
	}
}

// Pause is a function that pauses consuming messages.
func (c *Consumer) Pause() {
	if c.saramaConsumer == nil {
		panic("Consumer is not initialized")
	}

	if c.isClosed {
		panic("Consumer is closed")
	}
	// to pause the consumer working on StartConsume()
	c.paused <- true
}

// close is a function that closes the consumer.
// when consumer is paused, this function is called.
func (c *Consumer) close() {
	closeConsumerErr := (*c.saramaConsumer).Close()
	closeProducerErr := c.saramaProducer.Close()
	closeConnectionErr := c.connection.Close()

	if closeConsumerErr != nil {
		c.logger(closeConsumerErr.Error())
	}

	if closeProducerErr != nil {
		c.logger(closeProducerErr.Error())
	}

	if closeConnectionErr != nil {
		c.logger(closeConnectionErr.Error())
	}
}

// Stop is a function that stops consuming messages from Kafka.
func (c *Consumer) stop() {
	c.idleIdChan <- c.id
	c.isClosed = true
}
