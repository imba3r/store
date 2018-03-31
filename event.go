package thunder

import (
	"log"
	"fmt"
)

type EventHandler interface {
	Subscribe(key string) chan []byte
	SubscribeWithFunc(key string, f func() []byte) chan []byte
	Unsubscribe(key string, channel chan []byte)
	Publish(key string, data []byte)
}

type eventHandler struct {
	reg       *registry
	logEvents bool
}

type cmd struct {
	op      registryOperation
	key     string
	f       func() []byte
	channel chan []byte
	data    []byte
}

type topic struct {
	key         string
	f           func() []byte
	subscribers []chan []byte
}

type registry struct {
	cmdChan chan cmd
	topics  map[string]*topic
}

type registryOperation int

const (
	sub   registryOperation = iota
	pub
	unsub
)

func newEventHandler(logEvents bool) EventHandler {
	return &eventHandler{newRegistry(), logEvents}
}

func (e *eventHandler) Subscribe(key string) chan []byte {
	return e.reg.subscribe(key, nil)
}

func (e *eventHandler) SubscribeWithFunc(key string, f func() []byte) chan []byte {
	return e.reg.subscribe(key, f)
}

func (e *eventHandler) Unsubscribe(key string, channel chan []byte) {
	e.reg.unsubscribe(key, channel)
}

func (e *eventHandler) Publish(key string, data []byte) {
	if e.logEvents {
		log.Println(fmt.Sprintf("[PUBLISH:%s] %s", key, data))
	}
	e.reg.publish(key, data)
}

func (r *registry) subscribe(key string, f func() []byte) chan []byte {
	c := make(chan []byte, 1)
	r.cmdChan <- cmd{op: sub, channel: c, key: key, f: f}
	return c
}

func (r *registry) publish(key string, data []byte) {
	r.cmdChan <- cmd{op: pub, data: data, key: key}
}

func (r *registry) unsubscribe(key string, c chan []byte) {
	r.cmdChan <- cmd{op: unsub, channel: c, key: key}
}

func newRegistry() *registry {
	r := &registry{
		cmdChan: make(chan cmd),
		topics:  make(map[string]*topic),
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
			r.doSubscribe(cmd.key, cmd.f, cmd.channel)
		case unsub:
			r.doUnsubscribe(cmd.key, cmd.channel)
		}
	}
}

func (r *registry) doPublish(key string, data []byte) {
	t, exists := r.topics[key]
	if !exists {
		return
	}
	payload := data
	if t.f != nil {
		payload = t.f()
	}
	for _, sub := range t.subscribers {
		select {
		case sub <- payload:
		default:
		}
	}
}

func (r *registry) doSubscribe(key string, f func() []byte, channel chan []byte) {
	t, exists := r.topics[key]
	if !exists {
		r.topics[key] = &topic{
			key:         key,
			f:           f,
			subscribers: []chan []byte{channel},
		}
	} else {
		t.subscribers = append(t.subscribers, channel)
	}
}

func (r *registry) doUnsubscribe(topicName string, channel chan []byte) {
	defer close(channel)
	t, exists := r.topics[topicName]
	if !exists {
		return
	}
	position := -1
	for i, sub := range t.subscribers {
		if sub == channel {
			position = i
		}
	}
	if position >= 0 {
		t.subscribers[position] = t.subscribers[len(t.subscribers)-1]
		t.subscribers = t.subscribers[:len(t.subscribers)-1]
	}
	if (len(t.subscribers) == 0) {
		delete(r.topics, topicName)
	}
}
