package kafqueue

import (
	"context"
	"sync"

	qmanErr "github.com/violetpay-org/point3-quman/errors"
	qmanitem "github.com/violetpay-org/point3-quman/item"
	"github.com/violetpay-org/point3-quman/kafka.infra"
	qmanservices "github.com/violetpay-org/point3-quman/services"
)

// If Hub is not provided, it will create a new one with the given arguments
type Args struct {
	NumOfPartitions int
	Brokers         []string
	Serializer      kafka.KafkaMessageSerializer
	QueueName       qmanservices.QueueName
	Logger          func(string)
}

type KafkaQueueService struct {
	Hub       *kafka.Hub
	queueName qmanservices.QueueName
}

func NewQueue(
	args Args,
) (qmanservices.IQueueService, qmanservices.ILowLevelQueueOperator, error) {

	opts := []kafka.Opts{kafka.SetTopic(args.QueueName.String())}
	for _, broker := range args.Brokers {
		opts = append(opts, kafka.AddBroker(broker))
	}

	hub := kafka.NewHub(
		args.NumOfPartitions,
		args.Serializer,
		kafka.NewConfig(opts...), // with default config
		args.Logger,
	)

	queueService := &KafkaQueueService{
		Hub:       hub,
		queueName: args.QueueName,
	}

	return queueService, queueService, nil
}

func (o *KafkaQueueService) GetQueueName() qmanservices.QueueName {
	return o.queueName
}

func (o *KafkaQueueService) PopFromTheQueue(
	item qmanitem.IQueueItem,
	destination qmanservices.IQueueService,
) error {
	if destination == nil {
		return nil
	}

	err := destination.PushToTheQueue(item) // This will send the message to the queue
	return err
}

func (o *KafkaQueueService) PushToTheQueue(
	item qmanitem.IQueueItem,
) error {
	consumer := o.Hub.GetRandomConsumer()

	if consumer == nil {
		return qmanErr.ErrQueueNotPrepared(o.queueName.GetQueueName())
	}

	err := consumer.SendMessage(item, o.queueName.GetQueueName())
	return err
}

func (o *KafkaQueueService) StartQueue(
	onConsume qmanservices.QueueConsumeCallback,
	waitGroup *sync.WaitGroup,
	ctx *context.Context,
) error {
	hub := o.Hub

	if hub == nil || !hub.IsPrepared() {
		return qmanErr.ErrQueueNotPrepared(o.queueName.GetQueueName())
	}

	hub.StartConsumeAll(onConsume, waitGroup, ctx)

	return nil
}

func (o *KafkaQueueService) StopQueue(callback qmanservices.QueueStopCallback) error {
	hub := o.Hub

	if hub == nil || !hub.IsRunning() {
		return qmanErr.ErrQueueNotRunning(o.queueName.GetQueueName())
	}

	hub.Shutdown()

	return nil
}

func (o *KafkaQueueService) InsertMessage(item qmanitem.IQueueItem) error {
	return o.PushToTheQueue(item)
}
