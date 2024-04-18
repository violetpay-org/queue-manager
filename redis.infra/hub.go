package redis

import (
	"context"
	"fmt"
	"sync"

	redis_queue "github.com/asheswook/redis-queue"
	redis_core "github.com/redis/go-redis/v9"
	qmanitem "github.com/violetpay-org/point3-quman/item"
	qmanservices "github.com/violetpay-org/point3-quman/services"
)

type Hub struct {
	messageSerializer RedisMessageSerializer
	cluster           *redis_queue.SafeQueue
	logger            func(string)
	paused            chan bool
	isRunning         bool
	isPrepared        bool
}

func NewHub(
	messageSerializer RedisMessageSerializer,
	config *Config,
	logger func(string),
) *Hub {
	client := redis_core.NewClusterClient(
		&redis_core.ClusterOptions{
			Addrs: config.Addrs,
		},
	)

	queueWrapper := redis_queue.NewSafeQueue(
		&redis_queue.Config{
			Redis: client,
			Queue: struct {
				Name  string
				Retry int
			}{
				Name:  fmt.Sprintf("{%s}", config.QueueName),
				Retry: config.Retry,
			},
			Safe: struct {
				AckZSetName string
				TTL         int
			}{
				AckZSetName: fmt.Sprintf("{%s}:ack", config.QueueName),
				TTL:         config.TTL,
			},
		},
	)

	hub := &Hub{
		messageSerializer: messageSerializer,
		logger:            logger,
		cluster:           queueWrapper,
		paused:            make(chan bool),
		isRunning:         false,
		isPrepared:        false,
	}

	hub.isPrepared = true

	return hub
}

func (h *Hub) IsRunning() bool {
	return h.isRunning
}

func (h *Hub) IsPrepared() bool {
	return h.isPrepared
}

func (h *Hub) SendMessage(item qmanitem.IQueueItem) error {

	message, err := h.messageSerializer.QueueItemToRedisMessage(item)

	if err != nil {
		h.logger(err.Error())
	}

	return h.cluster.Push(message)
}

func (h *Hub) StartConsumeAll(
	onConsume qmanservices.QueueConsumeCallback,
	waitGroup *sync.WaitGroup,
	ctx *context.Context,
) {
	if !h.isPrepared || h.isRunning {
		return
	}

	h.isRunning = true

	context := *ctx

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		defer onConsume.OnStop(0)

		for {
			select {
			case <-context.Done():
				h.pause()
			case <-h.paused:
				h.isRunning = false
				return
			default:
				msg, _ := h.cluster.SafePop()

				if msg == nil {
					continue
				}

				serializedMessage, err := h.messageSerializer.RedisMessageToQueueItem(*msg)

				if err != nil {
					h.logger(err.Error())
				}

				go onConsume.OnConsumed(serializedMessage)

				err = msg.Ack()

				if err != nil {
					h.logger("while msg.Ack()" + err.Error())
				}
			}
		}
	}()
}

func (h *Hub) pause() {
	h.paused <- true
}

func (h *Hub) Shutdown() {
	h.pause()
}
