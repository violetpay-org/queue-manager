package config

import (
	"github.com/violetpay-org/queuemanager/internal/queueerror"
)

// QueueName is an enumeration of the different queues that the service can use.
// The string values are the names of the queues.
// The GetQueueName() method returns the string value of the queue name.
type QueueName int

var (
	// map of int to string
	queueNames map[int]string = make(map[int]string)
)

func (q QueueName) String() string {
	name, _ := queueNames[int(q)]
	return name
}

func (q QueueName) GetQueueName() string {
	return q.String()
}

func RegisterQueueName(name string, index int) (QueueName, error) {
	if _, ok := queueNames[index]; !ok {
		queueNames[index] = name
		return QueueName(index), nil
	}

	return QueueName(0), queueerror.ErrDuplicateQueueName()
}
