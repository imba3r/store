package badger

import (
	"fmt"
	"log"
	"bytes"

	"github.com/dgraph-io/badger"

	"github.com/imba3r/thunder/store"
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

var _ store.Store = &badgerStore{}

func New(path string) store.Store {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return &badgerStore{db: db, path: path};
}

func (bs *badgerStore) Document(key string) (store.Document, error) {
	if store.IsDocumentKey(key) {
		return &document{key, bs}, nil
	}
	return nil, fmt.Errorf("not a document path: %s", key)
}

func (bs *badgerStore) Collection(key string) (store.Collection, error) {
	if store.IsCollectionKey(key) {
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

func (c *collection) Add(data []byte) (store.Document, error) {
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

func (c *collection) Items(q store.Query, o store.Order, l store.Limit) ([]store.CollectionItem, error) {
	queryItems := q != (store.Query{})
	orderItems := o != (store.Order{})
	limitItems := l != (store.Limit{})
	limit := l.Limit + l.Offset

	var items []store.CollectionItem
	err := c.store.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		// Iterate with collection key as prefix.
		prefix := append([]byte(c.key), byte('/'))
		prefixLength := len(prefix)
		var itemCopy []byte
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			collectionItem := !bytes.ContainsAny(key[prefixLength:], "/")
			if collectionItem {
				// If items are unordered we may break during
				// iteration once the limit has been reached.
				if limitItems && !orderItems && len(items) == limit {
					break
				}

				// Copy the item contents as they are no longer
				// valid outside of the current transaction.
				itemCopy, err := item.ValueCopy(itemCopy)
				if err != nil {
					return err
				}

				// Filter out items that don't match the query (if any).
				if queryItems && !store.Matches(itemCopy, q){
					continue
				}
				items = append(items, store.CollectionItem{Key: string(key), Value: itemCopy})
			}
		}
		return nil
	})
	// Sort..
	if orderItems {
		store.OrderJSON(items, o)
	}
	// .. and limit.
	if limitItems && len(items) >= l.Offset {
		items = items[l.Offset:]
	}
	if limitItems && len(items) >= l.Limit {
		items = items[:l.Limit]
	}
	return items, err
}
