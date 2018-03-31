package thunder

type Document interface {
	Key() string
	Get() ([]byte, error)
	Set(data []byte) error
	Update(data []byte) error
	Delete() error
}

type Collection interface {
	Key() string
	All() ([]CollectionItem, error)
	Add(data []byte) (Document, error)
}

type CollectionItem struct {
	Key   string
	Value []byte
}

type Store interface {
	Document(path string) (Document, error)
	Collection(path string) (Collection, error)
	Close()
}
