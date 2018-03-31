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
	Query() Query
	Add(data []byte) (Document, error)
}

type Store interface {
	Document(path string) (Document, error)
	Collection(path string) (Collection, error)
	Close()
}

type Query interface {
	Items() ([]CollectionItem, error)
	OrderBy(key string, ascending bool) Query
	Limit(limit int) Query
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
