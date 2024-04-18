package kafqueue

import (
	"encoding/json"
	"reflect"

	"github.com/IBM/sarama"
	qmanitem "github.com/violetpay-org/point3-quman/item"
	qmanservices "github.com/violetpay-org/point3-quman/services"
)

type BaseKafkaQueueMessageSerializer struct {
	serializingType reflect.Type
}

func NewSerializer(target reflect.Type) *BaseKafkaQueueMessageSerializer {
	return &BaseKafkaQueueMessageSerializer{
		serializingType: target,
	}
}

func (p *BaseKafkaQueueMessageSerializer) QueueItemToProducerMessage(
	item qmanitem.IQueueItem,
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
) (qmanitem.IQueueItem, error) {
	target := reflect.New(p.serializingType).Interface()
	err := json.Unmarshal(message.Value, target)

	if err != nil {
		return nil, err
	}

	queueItem := qmanservices.MakeQueueItem(target)

	return queueItem, nil
}
