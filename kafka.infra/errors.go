package kafka

import (
	"errors"
)

var (
	ErrProducingMessage = errors.New("kafka: error producing message")
)
