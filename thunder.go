package thunder

import (
	"log"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/gorilla/websocket"
	"net/http"
)

type Store struct {
	db  *badger.DB
	reg *registry

	upgrader websocket.Upgrader
	mutex    sync.Mutex
}

type cmd struct {
	op  registryOperation
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

type registryOperation int

const (
	sub   registryOperation = iota
	pub
	unsub
)

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
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}
}

func (s *Store) Insert(key string, data []byte) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
	if err == nil {
		s.reg.publish(key, data)
	}
	return err
}

func (s *Store) Update(key string, data []byte) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
	if err == nil {
		s.reg.publish(key, data)
	}
	return err
}

func (s *Store) Delete(key string) error {
	err := s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
	if err == nil {
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

func (s *Store) Close() {
	s.db.Close()
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
			r.doPublish(cmd.key, cmd.data)
		case sub:
			r.doSubscribe(cmd.key, cmd.channel)
		case unsub:
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
