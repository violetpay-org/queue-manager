package kafka

import (
	"context"
	"github.com/violetpay-org/queuemanager/config"
	"github.com/violetpay-org/queuemanager/internal/queueerror"
	"github.com/violetpay-org/queuemanager/item"
	"github.com/violetpay-org/queuemanager/queue"
	"math/rand"
	"sync"
	"time"
)

// Hub is a struct that contains all the consumers.
type Hub struct {
	consumers         map[int]*Consumer // map of consumer id to consumer
	availID           chan int          // channel of available consumer ids
	consumersMax      int               // maximum number of consumers
	messageSerializer item.KafkaSerializer
	config            *config.KafkaConfig
	logger            func(string)
	publishOnly       bool // whether the hub is for publishing only
	isRunning         bool // whether the hub is running
	isInitialized     bool // whether the hub is initialized
}

func NewHub(
	maxConsumerCount int,
	messageSerializer item.KafkaSerializer,
	publishOnly bool,
	config *config.KafkaConfig,
	logger func(string),
) *Hub {
	hub := &Hub{}
	hub.Init(
		maxConsumerCount,
		messageSerializer,
		publishOnly,
		config,
		logger,
	)

	return hub
}

func (h *Hub) Init(
	maxConsumerCount int,
	messageSerializer item.KafkaSerializer,
	publishOnly bool,
	config *config.KafkaConfig,
	logger func(string),
) {
	h.consumersMax = maxConsumerCount
	h.messageSerializer = messageSerializer
	h.publishOnly = publishOnly
	h.config = config
	h.logger = logger

	if h.isInitialized {
		return
	}

	waitGroup := sync.WaitGroup{}
	mutex := sync.Mutex{}

	h.availID = make(chan int, h.consumersMax)
	h.consumers = make(map[int]*Consumer, h.consumersMax)

	// Create consumers
	for i := 0; i < h.consumersMax; i++ {
		h.availID <- i

		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()

			consumer := NewConsumer(h.availID, h.messageSerializer, h.publishOnly, h.config)
			consumer.SetLogger(h.logger)

			mutex.Lock()
			h.consumers[index] = consumer
			mutex.Unlock()
		}(i)
	}

	h.isInitialized = true

	waitGroup.Wait()
}

func (h *Hub) IsRunning() bool {
	return h.isRunning
}

func (h *Hub) IsPrepared() bool {
	return h.isInitialized
}

func (h *Hub) GetConsumer(consumerId int) *Consumer {

	mutex := sync.Mutex{}
	mutex.Lock()
	defer mutex.Unlock()

	return h.consumers[consumerId]
}

func (h *Hub) GetRandomConsumer() *Consumer {
	mutex := sync.Mutex{}

	mutex.Lock()
	consumer := h.consumers[0]
	mutex.Unlock()

	stop := make(chan interface{}, 1)

	go func() {
		stop <- <-time.After(time.Millisecond * 100)
	}()

	for {
		select {
		case <-stop:
			return consumer
		default:
			consumerId := rand.Intn(h.consumersMax)

			mutex.Lock()
			consumer = h.consumers[consumerId]
			mutex.Unlock()

			if consumer != nil {
				stop <- true
			}
		}
	}
}

func (h *Hub) MakeNewConsumer(ctx *context.Context) *Consumer {
	mutex := sync.Mutex{}

	defer mutex.Unlock()

	consumer := NewConsumer(h.availID, h.messageSerializer, h.publishOnly, h.config)
	mutex.Lock()
	h.consumers[consumer.id] = consumer
	return consumer
}

func (h *Hub) StartConsumeAll(
	callback queue.ConsumeCallback,
	waitGroup *sync.WaitGroup,
	ctx *context.Context,
) {
	if h.isRunning {
		return
	}

	for _, consumer := range h.consumers {
		waitGroup.Add(1)
		go consumer.StartConsume(callback, waitGroup, ctx)
	}

	h.isRunning = true
}

func (h *Hub) SendMessage(
	item item.Universal,
	topic string,
	consumerId int,
) error {
	if !h.isRunning {
		return queueerror.ErrQueueNotPrepared(topic)
	}

	mutex := sync.Mutex{}

	defer mutex.Unlock()

	mutex.Lock()
	consumer := h.GetConsumer(consumerId)
	err := consumer.SendMessage(item, topic)

	return err
}

func (h *Hub) Shutdown() {

	if !h.isRunning {
		return
	}

	// Shutdown waits until all consumers are stopped.
	mutex := sync.Mutex{}
	shutdownWaitGroup := sync.WaitGroup{}
	copyedConsumers := make(map[int]*Consumer, h.consumersMax) // To prevent deadlock while iterating over consumers

	for consumerId, consumer := range h.consumers {
		copyedConsumers[consumerId] = consumer
	}

	shutdownWaitGroup.Add(1)

	go func() {
		defer shutdownWaitGroup.Done()
		for consumerId, consumer := range copyedConsumers {
			consumer.Pause()
			mutex.Lock()
			delete(h.consumers, consumerId)
			mutex.Unlock()
		}
	}()

	shutdownWaitGroup.Wait()

	h.isRunning = false
}
