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
	key   string
	store *badgerStore
}

type collection struct {
	key   string
	store *badgerStore
}

type query struct {
	collection *collection
	limit      int
	orderBy    string
	ascending  bool
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

func (bs *badgerStore) Document(key string) (thunder.Document, error) {
	if thunder.IsDocumentKey(key) {
		return &document{key, bs}, nil
	}
	return nil, fmt.Errorf("not a document path: %s", key)
}

func (bs *badgerStore) Collection(key string) (thunder.Collection, error) {
	if thunder.IsCollectionKey(key) {
		return &collection{key, bs}, nil
	}
	return nil, fmt.Errorf("not a document path: %s", key)
}

func (bs *badgerStore) Close() {
	bs.db.Close();
}

func (d *document) Key() string {
	return d.key
}

func (d *document) Get() ([]byte, error) {
	var value []byte
	err := d.store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(d.key))
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
	return d.store.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(d.key), data)
	})
}

func (d *document) Update(data []byte) error {
	// TODO check if it exists
	return d.store.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(d.key), data)
	})
}

func (d *document) Delete() error {
	return d.store.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(d.key))
	})
}

func (c *collection) Key() string {
	return c.key
}

func (c *collection) Add(data []byte) (thunder.Document, error) {
	seq, err := c.store.db.GetSequence([]byte(c.key), 1)
	defer seq.Release()
	if err != nil {
		return nil, err
	}
	num, err := seq.Next()
	if err != nil {
		return nil, err
	}
	d, err := c.store.Document(fmt.Sprintf("%s/%d", c.key, num))
	if err != nil {
		return nil, err
	}
	return d, d.Set(data)
}

func (c *collection) Items(q thunder.Query) ([]thunder.CollectionItem, error) {
	var items []thunder.CollectionItem
	err := c.store.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		// Iterate with collection key as prefix.
		prefix := []byte(c.key)
		prefix = append(prefix, byte('/'))
		prefixLength := len(prefix)
		var itemCopyDst []byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			subCollection := bytes.ContainsAny(key[prefixLength:], "/")
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
	if q.OrderBy != "" {
		thunder.OrderBy(items, q.OrderBy, q.Ascending)
	}
	if q.Limit != 0 {
		return items[:q.Limit], err
	}
	return items, err
}
