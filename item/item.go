package item

type Universal interface {
	QueueItemToString() (string, error)
	QueueItemToJSON() (string, error)
}
