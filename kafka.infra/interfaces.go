package kafka

import (
	"github.com/IBM/sarama"
	qmanitem "github.com/violetpay-org/point3-quman/item"
)

type KafkaMessageSerializer interface {
	QueueItemToProducerMessage(item qmanitem.IQueueItem, topic string) (*sarama.ProducerMessage, error)
	ConsumerMessageToQueueItem(message *sarama.ConsumerMessage) (qmanitem.IQueueItem, error)
}
