package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	qmanitem "github.com/violetpay-org/point3-quman/item"
	"github.com/violetpay-org/point3-quman/kafka.infra"
)

func TestConsumerAndProducer(t *testing.T) {
	ctx := context.Background()

	config := kafka.NewConfig(
		kafka.SetTopic(kafka.TestQueueName),
		kafka.AddBrokers(kafka.TestBrokers),
	)

	hub := kafka.NewHub(
		1,
		&TestKafkaMessageSerializer{},
		config,
		func(msg string) {
			fmt.Println(msg)
		},
	)

	callback := TestKafkaConsumeCallback{
		Hub: hub,
		Ctx: &ctx,
		t:   t,
	}

	hub.StartConsumeAll(
		&callback,
		&sync.WaitGroup{},
		&ctx,
	)

	t.Run("KafkaPushMessageTest", func(t *testing.T) {
		hub.GetRandomConsumer().SendMessage(
			&TestQueueItem{
				Value: "test",
			},
			config.Topic,
		)

		// Wait for consumer to consume
		time.Sleep(1 * time.Second)
	})
}

// Test Mock Objects
// These objects are used to test the consumer

type TestQueueItem struct {
	Value string "json:'value'"
}

func (q *TestQueueItem) QueueItemToJSON() (string, error) {
	queueJson, err := json.Marshal(q)

	if err != nil {
		return "", err
	}

	return string(queueJson), nil
}

func (q *TestQueueItem) QueueItemToString() (string, error) {
	return q.Value, nil
}

type TestKafkaMessageSerializer struct{}

func (p *TestKafkaMessageSerializer) QueueItemToProducerMessage(
	item qmanitem.IQueueItem,
	topic string,
) (*sarama.ProducerMessage, error) {
	parsedItem, err := item.QueueItemToJSON()

	if err != nil {
		return nil, err
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(parsedItem),
	}

	return message, nil
}

func (p *TestKafkaMessageSerializer) ConsumerMessageToQueueItem(
	message *sarama.ConsumerMessage,
) (qmanitem.IQueueItem, error) {

	purchaseQueueItem := &TestQueueItem{}

	err := json.Unmarshal(message.Value, purchaseQueueItem)

	fmt.Println(message)

	if err != nil {
		return nil, err
	}

	return purchaseQueueItem, nil
}

type TestKafkaConsumeCallback struct {
	Hub *kafka.Hub
	Ctx *context.Context
	t   *testing.T
}

// TestKafkaConsumeCallback
// Used to test the consumer when it consumes a message
func (s *TestKafkaConsumeCallback) OnConsumed(message qmanitem.IQueueItem) {
	s.t.Run("KafkaConsumeMessageTest", func(t *testing.T) {

		value, err := message.QueueItemToString()

		if err != nil {
			s.t.Error("Message not parsed to string")
			return
		}

		if value != "test" {
			s.t.Error("Message is not test")
		}
	})
}

func (s *TestKafkaConsumeCallback) OnStop(consumerId int) {
	fmt.Println("[INFO] Consumer", consumerId, "stopped")

	newConsumer := s.Hub.MakeNewConsumer(s.Ctx)

	consumerWaitGroup := sync.WaitGroup{}

	consumerWaitGroup.Add(1)

	go newConsumer.StartConsume(
		s,
		&consumerWaitGroup,
		s.Ctx,
	)
}
