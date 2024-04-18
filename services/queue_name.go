package qmanservices

import (
	qmanErr "github.com/violetpay-org/point3-quman/errors"
)

// QueueName is an enumeration of the different queues that the service can use.
// The string values are the names of the queues.
// The GetQueueName() method returns the string value of the queue name.
type QueueName int

var (
	// map of int to string
	queue_names map[int]string = make(map[int]string)
)

func (q QueueName) String() string {
	name, _ := queue_names[int(q)]
	return name
}

func (q QueueName) GetQueueName() string {
	return q.String()
}

func RegisterQueueName(name string, index int) (QueueName, error) {
	if _, ok := queue_names[index]; !ok {
		queue_names[index] = name
		return QueueName(index), nil
	}

	return QueueName(0), qmanErr.ErrDuplicateQueueName()
}
