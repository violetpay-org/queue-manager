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
	publishOnly       bool

	// Status
	paused   chan bool
	isClosed bool
}

func NewConsumer(
	availIdChan chan int,
	messageSerializer KafkaMessageSerializer,
	publishOnly bool,
	conf *Config,
) (consumer *Consumer) {
	consumer = &Consumer{}
	consumer.messageSerializer = messageSerializer
	consumer.conf = conf
	consumer.publishOnly = publishOnly
	consumer.init(availIdChan)
	return
}

func (c *Consumer) setupProducer() error {
	producer, err := sarama.NewAsyncProducerFromClient(c.connection)

	if err != nil {
		return err
	}

	c.saramaProducer = producer
	return nil
}

func (c *Consumer) setupConsumer() error {
	consumer, err := sarama.NewConsumerFromClient(c.connection)

	if err != nil {
		return err
	}

	consumePartition, err := consumer.ConsumePartition(
		c.conf.Topic,
		int32(c.id),
		sarama.OffsetNewest,
	)

	if err != nil {
		return err
	}

	c.saramaConsumer = &consumePartition
	return nil
}

// Sets up conn with available id. Returns a function that revokes the given id and connection.
func (c *Consumer) setupConnection(availIdChan chan int) (revokeConn func(), err error) {
	conn, err := sarama.NewClient(c.conf.Brokers, c.conf.Conf)

	if err != nil {
		return nil, err
	}

	c.connection = conn
	c.idleIdChan = availIdChan
	c.id = <-c.idleIdChan

	revokeConn = func() {
		c.idleIdChan <- c.id
		c.connection.Close()
	}

	return revokeConn, nil
}

func (c *Consumer) init(availIdChan chan int) {

	revokeConn, err := c.setupConnection(availIdChan)
	if err != nil {
		panic(err)
	}

	if !c.publishOnly {
		err = c.setupConsumer()
	}

	if err != nil {
		revokeConn()
		panic(err)
	}

	if err := c.setupProducer(); err != nil {
		revokeConn()
		panic(err)
	}

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
	if c.connection == nil {
		panic("Connection is not established")
	}

	if c.saramaConsumer == nil {
		c.logger("Consumer is not initialized")
	}

	if c.isClosed {
		c.logger("Consumer is closed")
	}

	return c.saramaConsumer
}

func (c *Consumer) GetConnection() sarama.Client {
	if c.connection == nil {
		panic("Connnection is not established")
	}

	if c.isClosed {
		panic("Connection is closed")
	}

	return c.connection
}

func (c *Consumer) GetProducer() sarama.AsyncProducer {
	if c.connection == nil {
		panic("Connnection is not established")
	}

	if c.saramaProducer == nil {
		c.logger("Producer is not initialized")
	}

	if c.isClosed {
		c.logger("Connection is closed")
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
	fmt.Println("[INFO] Consumer", c.id, "started")

	if c.isClosed {
		panic("Consumer is closed")
	}

	defer waitGroup.Done()      // to notify the hub that the consumer is stopped
	defer callback.OnStop(c.id) // after the consumer is stopped
	defer c.stop()              // when the consumer is stopped

	ctx := *context

	if c.publishOnly {
		for {
			select {
			case <-(*context).Done():
				go c.Pause()
			case <-c.paused:
				c.close()
				close(c.paused)
				return nil, nil
			}
		}
	}

	saramaConsumer := *c.saramaConsumer
	for {
		select {
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
		return
	}

	if c.isClosed {
		return
	}
	// to pause the consumer working on StartConsume()
	c.paused <- true
}

// close is a function that closes the consumer.
// when consumer is paused, this function is called.
func (c *Consumer) close() {
	if c.isClosed {
		return
	}

	if !c.publishOnly {
		if err := c.closeConsumer(); err != nil {
			c.logger(err.Error())
		}
	}

	if err := c.closeProducer(); err != nil {
		c.logger(err.Error())
	}

	if err := c.connection.Close(); err != nil {
		c.logger(err.Error())
	}
}

func (c *Consumer) closeConsumer() error {
	if c.saramaConsumer == nil {
		return nil
	}

	return (*c.saramaConsumer).Close()
}
func (c *Consumer) closeProducer() error {
	if c.saramaProducer == nil {
		return nil
	}

	return c.saramaProducer.Close()
}

// Stop is a function that stops consuming messages from Kafka.
func (c *Consumer) stop() {
	c.idleIdChan <- c.id
	c.isClosed = true
}
