package thunder

import (
	"github.com/imba3r/thunder/pubsub"
	"github.com/imba3r/thunder/store"
)

type Thunder struct {
	Store  store.Store
	PubSub pubsub.PubSub
}

func New(s store.Store, logEvents bool) *Thunder {
	ps := pubsub.New(logEvents);
	return &Thunder{
		Store:  newAdapter(s, ps),
		PubSub: ps,
	}
}

func (t *Thunder) Open(enc store.Encoding) {
	t.Store.Open(enc)
}
