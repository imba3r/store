package thunder

import (
	"log"
	"encoding/json"
	"net/http"
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/gorilla/websocket"
)

type WebsocketOperation int

const (
	Subscribe WebsocketOperation = iota
	Insert
	Update
	Delete
	Snapshot
)

type Message struct {
	Key       string             `json:"key"`
	Operation WebsocketOperation `json:"operation,omitempty"`
	Payload   json.RawMessage    `json:"payload"`
}

type Store struct {
	db  *badger.DB
	reg *registry

	mutex    sync.Mutex
	upgrader *websocket.Upgrader
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
		upgrader: &websocket.Upgrader{
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

func (s *Store) HandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Fatal(err)
		}

		subscriptions := make(map[string]chan []byte)
		conn.SetCloseHandler(func(code int, text string) error {
			for key, channel := range subscriptions {
				s.Unsubscribe(key, channel)
			}
			return nil;
		})

		for {
			msgType, msg, err := conn.ReadMessage()
			if err != nil {
				log.Println("[ERR] websocket.Conn.ReadMessage", err)
				return
			}
			if msgType != websocket.TextMessage {
				log.Println("[ERR] messageType must be websocket.TextMessage")
				continue
			}

			var m Message
			err = json.Unmarshal(msg, &m)
			if err != nil {
				log.Println("[ERR] json.Unmarshal", err)
				continue
			}

			switch m.Operation {
			case Subscribe:
				subscriptions[m.Key] = s.Subscribe(m.Key)
				go s.listen(m.Key, subscriptions[m.Key], conn)
			case Update:
				s.Update(m.Key, m.Payload)
			case Insert:
				s.Insert(m.Key, m.Payload)
			case Delete:
				s.Delete(m.Key)
			case Snapshot:
				data, _ := s.Read(m.Key)
				s.writeMessage(conn, createAnswer(m.Key, data))
			}
		}
	}
}

func (s *Store) listen(key string, channel chan []byte, conn *websocket.Conn) {
	for {
		select {
		case m, ok := <-channel:
			if !ok {
				return
			}
			s.writeMessage(conn, createAnswer(key, m))
		}
	}
}

func (s *Store) writeMessage(conn *websocket.Conn, message []byte) {
	defer s.mutex.Unlock()
	s.mutex.Lock()

	err := conn.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		log.Println("[ERR] websocket.Conn.WriteMessage", err)
	}
}

func createAnswer(key string, data []byte) []byte {
	answer := &Message{
		Key:     key,
		Payload: data,
	}
	data, err := json.Marshal(answer)
	if err != nil {
		log.Println("[ERR] json.Marshal", err)
	}
	return data
}

type registryOperation int

const (
	sub   registryOperation = iota
	pub
	unsub
)

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
