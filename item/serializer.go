package queueitem

import (
	"encoding/json"
	"github.com/IBM/sarama"
	redisqueue "github.com/asheswook/redis-queue"
	"reflect"
)

// RedisSerializer is an interface that serializes redis queue items.
type RedisSerializer interface {
	QueueItemToRedisMessage(item Universal) (string, error)
	RedisMessageToQueueItem(message redisqueue.SafeMessage) (Universal, error)
}

// KafkaSerializer is an interface that serializes kafka queue items.
type KafkaSerializer interface {
	QueueItemToProducerMessage(item Universal, topic string) (*sarama.ProducerMessage, error)
	ConsumerMessageToQueueItem(message *sarama.ConsumerMessage) (Universal, error)
}

type BaseRedisQueueMessageSerializer struct {
	serializingType reflect.Type
}

func (p *BaseRedisQueueMessageSerializer) QueueItemToRedisMessage(item Universal) (string, error) {
	parsedItem, err := item.QueueItemToJSON()

	if err != nil {
		return "", err
	}

	return parsedItem, nil
}

func (p *BaseRedisQueueMessageSerializer) RedisMessageToQueueItem(message redisqueue.SafeMessage) (Universal, error) {
	target := reflect.New(p.serializingType).Interface()
	err := json.Unmarshal([]byte(message.Payload()), target)

	if err != nil {
		return nil, err
	}

	queueItem := MakeQueueItem(target)

	return queueItem, nil
}

type BaseKafkaQueueMessageSerializer struct {
	serializingType reflect.Type
}

func (p *BaseKafkaQueueMessageSerializer) QueueItemToProducerMessage(
	item Universal,
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

func (p *BaseKafkaQueueMessageSerializer) ConsumerMessageToQueueItem(
	message *sarama.ConsumerMessage,
) (Universal, error) {
	target := reflect.New(p.serializingType).Interface()
	err := json.Unmarshal(message.Value, target)

	if err != nil {
		return nil, err
	}

	queueItem := MakeQueueItem(target)

	return queueItem, nil
}

func NewRedisSerializer(target reflect.Type) *BaseRedisQueueMessageSerializer {
	return &BaseRedisQueueMessageSerializer{
		serializingType: target,
	}
}

func NewKafkaSerializer(target reflect.Type) *BaseKafkaQueueMessageSerializer {
	return &BaseKafkaQueueMessageSerializer{
		serializingType: target,
	}
}
