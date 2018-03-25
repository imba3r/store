package thunder

import (
	"log"

	"github.com/dgraph-io/badger"
)

type Store struct {
	db  *badger.DB
	reg *registry
}

func (s *Store) Close() {
	s.db.Close()
}

func NewStore(path string) *Store {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	return &Store{
		db:  db,
		reg: Newregistry(),
	}
}

func (s *Store) Insert(key string, data []byte) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
	if err == nil {
		log.Println("[INSERT] Publishing", key, string(data))
		s.reg.publish(key, data)
	}
	return err
}

func (s *Store) Update(key string, data []byte) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
	if err == nil {
		log.Println("[UPDATE] Publishing", key, string(data))
		s.reg.publish(key, data)
	}
	return err
}

func (s *Store) Delete(key string) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	if err == nil {
		log.Println("[DELETE] Publishing", key)
		s.reg.publish(key, nil)
	}
	return err
}

func (s *Store) Read(key string) ([]byte, error) {
	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
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

func (s *Store) Subscribe(key string) chan []byte {
	return s.reg.subscribe(key)
}

func (s *Store) Unsubscribe(key string, channel chan []byte) {
	s.reg.unsubscribe(key, channel)
}

type operation int

const (
	sub   operation = iota
	pub
	unsub
)

type cmd struct {
	op  operation
	key string

	channel chan []byte
	data    []byte
}

type entry struct {
	key         string
	subscribers []chan []byte
}

type registry struct {
	cmdChan chan cmd
	entries map[string]*entry
}

func (r *registry) subscribe(key string) chan []byte {
	c := make(chan []byte, 1)
	r.cmdChan <- cmd{op: sub, channel: c, key: key}
	return c
}

func (r *registry) publish(key string, data []byte) {
	r.cmdChan <- cmd{op: pub, data: data, key: key}
}

func (r *registry) unsubscribe(key string, c chan []byte) {
	r.cmdChan <- cmd{op: unsub, channel: c, key: key}
}

func Newregistry() *registry {
	r := &registry{
		cmdChan: make(chan cmd),
		entries: make(map[string]*entry),
	}
	go r.start()
	return r
}

func (r *registry) start() {
	for cmd := range r.cmdChan {
		switch cmd.op {
		case pub:
			log.Println("[REG] Handling pub")
			r.doPublish(cmd.key, cmd.data)
		case sub:
			log.Println("[REG] Handling sub")
			r.doSubscribe(cmd.key, cmd.channel)
		case unsub:
			log.Println("[REG] Handling unsub")
			r.doUnsubscribe(cmd.key, cmd.channel)
		}
	}
}

func (r *registry) doPublish(key string, data []byte) {
	e, exists := r.entries[key]
	if !exists {
		return
	}
	for _, sub := range e.subscribers {
		select {
		case sub <- data:
		default:
		}
	}
}

func (r *registry) doSubscribe(key string, channel chan []byte) {
	e, exists := r.entries[key]
	if !exists {
		r.entries[key] = &entry{
			key:         key,
			subscribers: []chan []byte{channel},
		}
		log.Println("[SUB]", key, "didn't exist yet.")
	} else {
		e.subscribers = append(e.subscribers, channel)
	}
}

func (r *registry) doUnsubscribe(topicName string, channel chan []byte) {
	defer close(channel)
	e, exists := r.entries[topicName]
	if !exists {
		return
	}
	position := -1
	for i, sub := range e.subscribers {
		if sub == channel {
			position = i
		}
	}
	if position >= 0 {
		e.subscribers[position] = e.subscribers[len(e.subscribers)-1]
		e.subscribers = e.subscribers[:len(e.subscribers)-1]
	}
	if (len(e.subscribers) == 0) {
		delete(r.entries, topicName)
	}
}
