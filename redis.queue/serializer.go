package redqueue

import (
	"encoding/json"
	"reflect"

	redis_queue "github.com/asheswook/redis-queue"
	qmanitem "github.com/violetpay-org/point3-quman/item"
	"github.com/violetpay-org/point3-quman/redis.infra"
	qmanservices "github.com/violetpay-org/point3-quman/services"
)

type BaseRedisQueueMessageSerializer struct {
	serializingType reflect.Type
}

func NewSerializer(target reflect.Type) redis.RedisMessageSerializer {
	return &BaseRedisQueueMessageSerializer{
		serializingType: target,
	}
}

func (p *BaseRedisQueueMessageSerializer) QueueItemToRedisMessage(item qmanitem.IQueueItem) (string, error) {
	parsedItem, err := item.QueueItemToJSON()

	if err != nil {
		return "", err
	}

	return parsedItem, nil
}

func (p *BaseRedisQueueMessageSerializer) RedisMessageToQueueItem(message redis_queue.SafeMessage) (qmanitem.IQueueItem, error) {
	target := reflect.New(p.serializingType).Interface()
	err := json.Unmarshal([]byte(message.Payload()), target)

	if err != nil {
		return nil, err
	}

	queueItem := qmanservices.MakeQueueItem(target)

	return queueItem, nil
}
