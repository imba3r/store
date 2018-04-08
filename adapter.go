package thunder

import (
	"github.com/imba3r/thunder/store"
	"github.com/imba3r/thunder/pubsub"
)

type adapter struct {
	store  store.Store
	pubsub pubsub.PubSub
}

type document struct {
	document store.Document
	pubsub   pubsub.PubSub
}

type collection struct {
	collection store.Collection
	pubsub     pubsub.PubSub
}

var _ store.Store = &adapter{}

func newAdapter(store store.Store, pubsub pubsub.PubSub) *adapter {
	return &adapter{store, pubsub}
}

func (a *adapter) Open(enc store.Encoding) error {
	return a.store.Open(enc)
}

func (a *adapter) Document(path string) (store.Document, error) {
	d, err := a.store.Document(path)
	return &document{d, a.pubsub}, err
}

func (a *adapter) Collection(path string) (store.Collection, error) {
	c, err := a.store.Collection(path)
	return &collection{c, a.pubsub}, err
}

func (a *adapter) Close() {
	a.store.Close()
}

func (d *document) Key() string {
	return d.document.Key();
}

func (d *document) Get() ([]byte, error) {
	return d.document.Get()
}

func (d *document) Set(data []byte) error {
	err := d.document.Set(data)
	if err == nil {
		collectionKey := store.CollectionKey(d.document.Key())
		d.pubsub.Publish(collectionKey, data)
		d.pubsub.Publish(d.document.Key(), data)
	}
	return err
}

func (d *document) Update(data []byte) error {
	err := d.document.Update(data)
	if err == nil {
		collectionKey := store.CollectionKey(d.document.Key())
		d.pubsub.Publish(collectionKey, data)
		d.pubsub.Publish(d.document.Key(), data)
	}
	return err
}

func (d *document) Delete() error {
	err := d.document.Delete()
	if err == nil {
		collectionKey := store.CollectionKey(d.document.Key())
		d.pubsub.Publish(collectionKey, nil)
		d.pubsub.Publish(d.document.Key(), nil)
	}
	return err
}

func (c *collection) Key() string {
	return c.collection.Key();
}

func (c *collection) Add(data []byte) (store.Document, error) {
	doc, err := c.collection.Add(data)
	if err == nil {
		c.pubsub.Publish(c.collection.Key(), data)
		c.pubsub.Publish(doc.Key(), data)
	}
	return doc, err
}

func (c *collection) Items(q store.Query, o store.Order, l store.Limit) ([]store.CollectionItem, error) {
	return c.collection.Items(q, o, l)
}
