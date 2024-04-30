package kafkaqueue

import (
	"context"
	"github.com/violetpay-org/queuemanager/config"
	"github.com/violetpay-org/queuemanager/internal/queue/kafka"
	"github.com/violetpay-org/queuemanager/internal/queueerror"
	"github.com/violetpay-org/queuemanager/item"
	queue2 "github.com/violetpay-org/queuemanager/queue"
	"sync"
)

// If Hub is not provided, it will create a new one with the given arguments
type Args struct {
	NumOfPartitions int
	Brokers         []string
	PublishOnly     bool
	Serializer      queueitem.KafkaSerializer
	QueueName       queuemanagerconfig.QueueName
	Logger          func(string)
}

type Service struct {
	Hub       *kafka.Hub
	queueName queuemanagerconfig.QueueName
}

func NewQueue(
	args Args,
) (queue2.Service, queue2.ILowLevelQueueOperator, error) {

	opts := []queuemanagerconfig.KafkaOpts{queuemanagerconfig.SetKafkaTopic(args.QueueName.String())}
	for _, broker := range args.Brokers {
		opts = append(opts, queuemanagerconfig.AddKafkaBroker(broker))
	}

	hub := kafka.NewHub(
		args.NumOfPartitions,
		args.Serializer,
		args.PublishOnly,
		queuemanagerconfig.NewKafkaConfig(opts...), // with default config
		args.Logger,
	)

	queueService := &Service{
		Hub:       hub,
		queueName: args.QueueName,
	}

	return queueService, queueService, nil
}

func (o *Service) GetQueueName() queuemanagerconfig.QueueName {
	return o.queueName
}

func (o *Service) PopFromTheQueue(
	item queueitem.Universal,
	destination queue2.Service,
) error {
	if destination == nil {
		return nil
	}

	err := destination.PushToTheQueue(item) // This will send the message to the queue
	return err
}

func (o *Service) PushToTheQueue(
	item queueitem.Universal,
) error {
	consumer := o.Hub.GetRandomConsumer()

	if consumer == nil {
		return queueerror.ErrQueueNotPrepared(o.queueName.GetQueueName())
	}

	err := consumer.SendMessage(item, o.queueName.GetQueueName())
	return err
}

func (o *Service) StartQueue(
	onConsume queue2.ConsumeCallback,
	waitGroup *sync.WaitGroup,
	ctx *context.Context,
) error {
	hub := o.Hub

	if hub == nil || !hub.IsPrepared() {
		return queueerror.ErrQueueNotPrepared(o.queueName.GetQueueName())
	}

	hub.StartConsumeAll(onConsume, waitGroup, ctx)

	return nil
}

func (o *Service) StopQueue(callback queue2.StopCallback) error {
	hub := o.Hub

	if hub == nil || !hub.IsRunning() {
		return queueerror.ErrQueueNotRunning(o.queueName.GetQueueName())
	}

	hub.Shutdown()

	callback.OnStop()

	return nil
}

func (o *Service) InsertMessage(item queueitem.Universal) error {
	return o.PushToTheQueue(item)
}
