package thunder

type EventHandler struct {
	reg *registry
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

func newEventHandler() *EventHandler {
	return &EventHandler{newRegistry()}
}

func (e *EventHandler) Subscribe(key string) chan []byte {
	return e.reg.subscribe(key)
}

func (e *EventHandler) Unsubscribe(key string, channel chan []byte) {
	e.reg.unsubscribe(key, channel)
}

func (e *EventHandler) Publish(key string, data []byte) {
	e.reg.publish(key, data)
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

func newRegistry() *registry {
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
