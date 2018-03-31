package thunder

type adapter struct {
	store        Store
	eventHandler EventHandler
}

type document struct {
	document     Document
	eventHandler EventHandler
}

type collection struct {
	collection   Collection
	eventHandler EventHandler
}

var _ Store = &adapter{}

func newAdapter(store Store, eventHandler EventHandler) *adapter {
	return &adapter{store, eventHandler}
}

func (a *adapter) Document(path string) (Document, error) {
	d, err := a.store.Document(path)
	return &document{d, a.eventHandler}, err
}

func (a *adapter) Collection(path string) (Collection, error) {
	c, err := a.store.Collection(path)
	return &collection{c, a.eventHandler}, err
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
		collectionKey := CollectionKey(d.document.Key())
		d.eventHandler.Publish(collectionKey, data)
		d.eventHandler.Publish(d.document.Key(), data)
	}
	return err
}

func (d *document) Update(data []byte) error {
	err := d.document.Update(data)
	if err == nil {
		collectionKey := CollectionKey(d.document.Key())
		d.eventHandler.Publish(collectionKey, data)
		d.eventHandler.Publish(d.document.Key(), data)
	}
	return err
}

func (d *document) Delete() error {
	err := d.document.Delete()
	if err == nil {
		collectionKey := CollectionKey(d.document.Key())
		d.eventHandler.Publish(collectionKey, nil)
		d.eventHandler.Publish(d.document.Key(), nil)
	}
	return err
}

func (c *collection) Key() string {
	return c.collection.Key();
}

func (c *collection) Add(data []byte) (Document, error) {
	doc, err := c.collection.Add(data)
	if err == nil {
		c.eventHandler.Publish(c.collection.Key(), data)
		c.eventHandler.Publish(doc.Key(), data)
	}
	return doc, err
}

func (c *collection) Query() Query {
	return c.collection.Query()
}
