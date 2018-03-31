package badger

import (
	"fmt"
	"log"

	"github.com/dgraph-io/badger"

	"github.com/imba3r/thunder"
	"bytes"
)

type badgerStore struct {
	path string
	db   *badger.DB
}

type document struct {
	key string
	*badgerStore
}

type collection struct {
	key string
	*badgerStore
}

var _ thunder.Store = &badgerStore{}

func New(path string) thunder.Store {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return &badgerStore{db: db, path: path};
}

func (bs *badgerStore) Document(path string) (thunder.Document, error) {
	if thunder.IsDocumentKey(path) {
		return &document{path, bs}, nil
	}
	return nil, fmt.Errorf("not a document path: %s", path)
}

func (bs *badgerStore) Collection(path string) (thunder.Collection, error) {
	if thunder.IsCollectionKey(path) {
		return &collection{path, bs}, nil
	}
	return nil, fmt.Errorf("not a document path: %s", path)
}

func (bs *badgerStore) Close() {
	bs.db.Close();
}

func (d *document) Key() string {
	return d.key
}

func (d *document) Get() ([]byte, error) {
	var value []byte
	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(d.path))
		if err != nil {
			return err
		}
		v, err := item.Value()
		if err != nil {
			return err
		}
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (d *document) Set(data []byte) error {
	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(d.path), data)
	})
}

func (d *document) Update(data []byte) error {
	// TODO check if it exists
	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(d.path), data)
	})
}

func (d *document) Delete() error {
	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(d.key))
	})
}

func (c *collection) Key() string {
	return c.key
}

func (c *collection) Add(data []byte) (thunder.Document, error) {
	seq, err := c.db.GetSequence([]byte(c.key), 1)
	defer seq.Release()
	if err != nil {
		return nil, err
	}
	num, err := seq.Next()
	if err != nil {
		return nil, err
	}
	d, err := c.Document(fmt.Sprintf("%s/%d", c.key, num))
	if err != nil {
		return nil, err
	}
	return d, d.Set(data)
}

func (c *collection) All() ([]thunder.CollectionItem, error) {
	err := c.db.View(func(txn *badger.Txn) error {
		var items []thunder.CollectionItem

		// Create iterator that does not prefetch values.
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		// Iterate with collection key as prefix.
		prefix := []byte(c.key)
		prefixLength := len(prefix)
		var itemCopyDst []byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			subCollection := bytes.ContainsAny(key[:prefixLength], "/")
			if !subCollection {
				itemCopyDst, err := item.ValueCopy(itemCopyDst)
				if err != nil {
					return err
				}
				items = append(items, thunder.CollectionItem{Key: string(key), Value: itemCopyDst})
			}
		}
		return nil
	})
	return nil, err
}