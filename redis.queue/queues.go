package redqueue

import (
	"context"
	"sync"

	qmanErr "github.com/violetpay-org/point3-quman/errors"
	qmanitem "github.com/violetpay-org/point3-quman/item"
	"github.com/violetpay-org/point3-quman/redis.infra"
	qmanservices "github.com/violetpay-org/point3-quman/services"
)

type Args struct {
	MessageSerializer redis.RedisMessageSerializer
	QueueName         qmanservices.QueueName
	Logger            func(string)
	Brokers           []string
}

type RedisQueueService struct {
	Hub       *redis.Hub
	queueName qmanservices.QueueName
}

func NewQueue(args Args) (qmanservices.IQueueService, qmanservices.ILowLevelQueueOperator, error) {

	hub := redis.NewHub(
		args.MessageSerializer,
		redis.NewConfig(
			redis.SetQueueName(args.QueueName.GetQueueName()),
			redis.AddBrokers(args.Brokers),
			redis.AddRetry(3),
			redis.AddTTL(60),
		),
		args.Logger,
	)

	queueService := &RedisQueueService{
		Hub:       hub,
		queueName: args.QueueName,
	}

	return queueService, queueService, nil
}

func (s *RedisQueueService) GetQueueName() qmanservices.QueueName {
	return s.queueName
}

func (s *RedisQueueService) PopFromTheQueue(
	item qmanitem.IQueueItem,
	destination qmanservices.IQueueService,
) error {
	if destination == nil {
		return nil
	}

	err := destination.PushToTheQueue(item)
	return err
}

func (s *RedisQueueService) PushToTheQueue(item qmanitem.IQueueItem) error {
	err := s.Hub.SendMessage(item)
	return err
}

func (s *RedisQueueService) StartQueue(
	onConsume qmanservices.QueueConsumeCallback,
	waitGroup *sync.WaitGroup,
	ctx *context.Context,
) error {
	hub := s.Hub

	if hub == nil || !hub.IsPrepared() {
		return qmanErr.ErrQueueNotPrepared(s.queueName.GetQueueName())
	}

	hub.StartConsumeAll(
		onConsume,
		waitGroup,
		ctx,
	)

	return nil
}

func (s *RedisQueueService) StopQueue(callback qmanservices.QueueStopCallback) error {
	hub := s.Hub

	if hub == nil || !hub.IsRunning() {
		return qmanErr.ErrQueueNotRunning(s.queueName.GetQueueName())
	}

	hub.Shutdown()

	return nil
}

func (s *RedisQueueService) InsertMessage(item qmanitem.IQueueItem) error {
	return s.PushToTheQueue(item)
}
