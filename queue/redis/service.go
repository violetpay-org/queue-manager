package redisqueue

import (
	"context"
	"github.com/violetpay-org/queuemanager/config"
	"github.com/violetpay-org/queuemanager/internal/queue/redis"
	"github.com/violetpay-org/queuemanager/internal/queueerror"
	"github.com/violetpay-org/queuemanager/item"
	queue2 "github.com/violetpay-org/queuemanager/queue"
	"sync"
)

type Args struct {
	MessageSerializer item.RedisSerializer
	QueueName         config.QueueName
	Logger            func(string)
	Brokers           []string
}

type Service struct {
	Hub       *redis.Hub
	queueName config.QueueName
}

func NewQueue(args Args) (*Service, *Service, error) {
	hub := redis.NewHub(
		args.MessageSerializer,
		config.NewRedisConfig(
			config.SetRedisQueueName(args.QueueName.GetQueueName()),
			config.AddRedisBrokers(args.Brokers),
			config.AddRedisRetry(3),
			config.AddRedisTTL(60),
		),
		args.Logger,
	)

	queueService := &Service{
		Hub:       hub,
		queueName: args.QueueName,
	}

	return queueService, queueService, nil
}

func (s *Service) GetQueueName() config.QueueName {
	return s.queueName
}

func (s *Service) PopFromTheQueue(
	item item.Universal,
	destination queue2.Service,
) error {
	if destination == nil {
		return nil
	}

	err := destination.PushToTheQueue(item)
	return err
}

func (s *Service) PushToTheQueue(item item.Universal) error {
	err := s.Hub.SendMessage(item)
	return err
}

func (s *Service) StartQueue(
	onConsume queue2.ConsumeCallback,
	waitGroup *sync.WaitGroup,
	ctx *context.Context,
) error {
	hub := s.Hub

	if hub == nil || !hub.IsPrepared() {
		return queueerror.ErrQueueNotPrepared(s.queueName.GetQueueName())
	}

	hub.StartConsumeAll(
		onConsume,
		waitGroup,
		ctx,
	)

	return nil
}

func (s *Service) StopQueue(callback queue2.StopCallback) error {
	hub := s.Hub

	if hub == nil || !hub.IsRunning() {
		return queueerror.ErrQueueNotRunning(s.queueName.GetQueueName())
	}

	hub.Shutdown()

	callback.OnStop()

	return nil
}

func (s *Service) InsertMessage(item item.Universal) error {
	return s.PushToTheQueue(item)
}
