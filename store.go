package thunder

import "encoding/json"

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

type Store interface {
	Document(path string) (Document, error)
	Collection(path string) (Collection, error)
	Close()
}

type CollectionItem struct {
	Key   string
	Value []byte
}

func (i CollectionItem) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Key   string          `json:"key"`
		Value json.RawMessage `json:"value"`
	}{
		Key:   i.Key,
		Value: i.Value,
	})
}
