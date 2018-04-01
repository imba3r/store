package store

import (
	"encoding/json"
	"strings"
	"log"
	"fmt"
)

type Store interface {
	Document(path string) (Document, error)
	Collection(path string) (Collection, error)
	Close()
}

type Document interface {
	Key() string
	Get() ([]byte, error)
	Set(data []byte) error
	Update(data []byte) error
	Delete() error
}

type Collection interface {
	Key() string
	Items(Query, Order, Limit) ([]CollectionItem, error)
	Add(data []byte) (Document, error)
}

type Limit struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

type Order struct {
	OrderBy   string `json:"orderBy"`
	Ascending bool   `json:"ascending"`
}

type Query struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
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

func IsCollectionKey(path string) bool {
	return len(strings.Split(path, "/"))%2 == 1;
}

func IsDocumentKey(path string) bool {
	return !IsCollectionKey(path);
}

func CollectionKey(documentKey string) string {
	split := strings.Split(documentKey, "/")
	if len(split) < 2 || len(split)%2 == 1 {
		// FIXME hmm
		log.Fatal("not a document key: ", documentKey)
	}
	return strings.TrimSuffix(documentKey, fmt.Sprintf("/%s", split[len(split)-1]))
}
