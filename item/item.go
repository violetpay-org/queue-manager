package queueitem

type Universal interface {
	QueueItemToString() (string, error)
	QueueItemToJSON() (string, error)
}
