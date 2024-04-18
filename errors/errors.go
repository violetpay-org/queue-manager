package qmanErr

import (
	"errors"
)

func ErrQueueFactoryWaitGroupNil() error {
	return errors.New("WaitGroup is nil")
}

func ErrQueueNotFound(queueName string) error {
	return errors.New("Queue not found: " + queueName)
}

func ErrQueueNotPrepared(queueName string) error {
	return errors.New("Queue not prepared: " + queueName)
}

func ErrQueueNotRunning(queueName string) error {
	return errors.New("Queue not running: " + queueName)
}

func ErrDuplicateQueue() error {
	return errors.New("Queue already exists")
}

func ErrDuplicateQueueName() error {
	return errors.New("Queue name with same index already exists")
}

func ErrUnknownQueueName() error {
	return errors.New("Unknown queue name")
}
