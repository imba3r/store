package thunder

type Value interface {
	Key() string
}

type Document interface {
	Get() ([]byte, error)
	Set(data []byte) error
	Update(data []byte) error
	Delete() error
}

type Collection interface {
	Add(data []byte) (Document, error)
}

type Store interface {
	Document(path string) (Document, error)
	Collection(path string) (Collection, error)
	Close()
}
