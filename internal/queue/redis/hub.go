package redis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	queuemanagerconfig "github.com/violetpay-org/queuemanager/config"
	"github.com/violetpay-org/queuemanager/item"
	"github.com/violetpay-org/queuemanager/queue"
	"sync"

	redisqueue "github.com/asheswook/redis-queue"
)

type Hub struct {
	messageSerializer queueitem.RedisSerializer
	cluster           *redisqueue.SafeQueue
	logger            func(string)
	paused            chan bool
	isRunning         bool
	isPrepared        bool
}

func NewHub(
	messageSerializer queueitem.RedisSerializer,
	config *queuemanagerconfig.RedisConfig,
	logger func(string),
) *Hub {
	hub := &Hub{
		paused:     make(chan bool),
		isRunning:  false,
		isPrepared: false,
	}
	hub.init(
		messageSerializer,
		config,
		logger,
	)

	return hub
}

func (h *Hub) init(
	messageSerializer queueitem.RedisSerializer,
	config *queuemanagerconfig.RedisConfig,
	logger func(string),
) {
	h.messageSerializer = messageSerializer
	h.logger = logger

	client := redis.NewClusterClient(
		&redis.ClusterOptions{
			Addrs: config.Addrs,
		},
	)

	queueWrapper := redisqueue.NewSafeQueue(
		&redisqueue.Config{
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

	h.cluster = queueWrapper

	h.isPrepared = true
}

func (h *Hub) IsRunning() bool {
	return h.isRunning
}

func (h *Hub) IsPrepared() bool {
	return h.isPrepared
}

func (h *Hub) SendMessage(item queueitem.Universal) error {

	message, err := h.messageSerializer.QueueItemToRedisMessage(item)

	if err != nil {
		h.logger(err.Error())
	}

	return h.cluster.Push(message)
}

func (h *Hub) StartConsumeAll(
	onConsume innerqueue.ConsumeCallback,
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
