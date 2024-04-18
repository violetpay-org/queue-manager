package redis

import (
	redis_queue "github.com/asheswook/redis-queue"
	qmanitem "github.com/violetpay-org/point3-quman/item"
)

type RedisMessageSerializer interface {
	QueueItemToRedisMessage(item qmanitem.IQueueItem) (string, error)
	RedisMessageToQueueItem(message redis_queue.SafeMessage) (qmanitem.IQueueItem, error)
}
