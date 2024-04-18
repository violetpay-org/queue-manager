package kafka

import (
	"context"
	"math/rand"
	"sync"
	"time"

	qmanErr "github.com/violetpay-org/point3-quman/errors"
	qmanitem "github.com/violetpay-org/point3-quman/item"
	qmanservices "github.com/violetpay-org/point3-quman/services"
)

// Hub is a struct that contains all the consumers.
type Hub struct {
	consumers         map[int]*Consumer // map of consumer id to consumer
	availID           chan int          // channel of available consumer ids
	consumersMax      int               // maximum number of consumers
	messageSerializer KafkaMessageSerializer
	config            *Config
	logger            func(string)
	isRunning         bool // whether the hub is running
	isInitialized     bool // whether the hub is initialized
}

// NewHub is a function that returns a new Hub.
// maxConsumerCount is the maximum number of consumers that can be created.
func NewHub(
	maxConsumerCount int,
	messageSerializer KafkaMessageSerializer,
	config *Config,
	logger func(string),
) *Hub {
	Hub := &Hub{
		consumersMax:      maxConsumerCount,
		messageSerializer: messageSerializer,
		logger:            logger,
		config:            config,
	}
	Hub.init()
	return Hub
}

func (h *Hub) init() {

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

			consumer := NewConsumer(h.availID, h.messageSerializer, h.config)
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

	consumer := NewConsumer(h.availID, h.messageSerializer, h.config)
	mutex.Lock()
	h.consumers[consumer.id] = consumer
	return consumer
}

func (h *Hub) StartConsumeAll(
	callback qmanservices.QueueConsumeCallback,
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
	item qmanitem.IQueueItem,
	topic string,
	consumerId int,
) error {
	if !h.isRunning {
		return qmanErr.ErrQueueNotPrepared(topic)
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
