package qmanitem

type IQueueItem interface {
	QueueItemToString() (string, error)
	QueueItemToJSON() (string, error)
}
