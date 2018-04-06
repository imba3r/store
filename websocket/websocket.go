package websocket

import (
	"encoding/json"
	"net/http"
	"log"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/imba3r/thunder"
	"github.com/imba3r/thunder/store"
)

type WebSocketOperation string

const (
	// Incoming
	Subscribe WebSocketOperation = "SUBSCRIBE"
	Add       WebSocketOperation = "ADD"
	Set       WebSocketOperation = "SET"
	Update    WebSocketOperation = "UPDATE"
	Delete    WebSocketOperation = "DELETE"

	// Outgoing
	ValueChange WebSocketOperation = "VALUE_CHANGE"

	// Incoming & Outgoing
	Snapshot WebSocketOperation = "SNAPSHOT"
)

type WebSocketHandler struct {
	thunder  *thunder.Thunder
	upgrader websocket.Upgrader
	mutex    sync.Mutex
}

type WebSocketMessage struct {
	Operation           WebSocketOperation  `json:"operation"`
	OperationParameters OperationParameters `json:"operationParameters,omitempty"`
	Key                 string              `json:"key"`
	RequestID           uint64              `json:"requestId"`
	TransactionID       uint64              `json:"transactionId"`
	Error               Error               `json:"error"`
	Payload             json.RawMessage     `json:"payload,omitempty"`
	PayloadMetadata     PayloadMetadata     `json:"payloadMetadata,omitempty"`
}

type OperationParameters struct {
	Query store.Query `json:"query"`
	Limit store.Limit `json:"limit"`
	Order store.Order `json:"offset"`
}

type Error struct {
	Message string `json:"message"`
}

type PayloadMetadata struct {
	Exists bool `json:"exists"`
}

func NewWebSocketHandler(thunder *thunder.Thunder) *WebSocketHandler {
	return &WebSocketHandler{
		thunder: thunder,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	};
}

func (h *WebSocketHandler) HandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := h.upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("[ERR] websocket.Upgrader.Upgrade", err)
			return
		}

		// Create a map of subscriptions (mapped by key) to the underlying storage.
		// Unsubscribe from them in the connection's close handler.
		subscriptions := make(map[string]chan []byte)
		conn.SetCloseHandler(func(code int, text string) error {
			for key, channel := range subscriptions {
				h.thunder.PubSub.Unsubscribe(key, channel)
			}
			return nil;
		})

		for {
			msgType, msg, err := conn.ReadMessage()
			if err != nil {
				log.Println("[ERR] websocket.Conn.ReadMessage", err)
				continue
			}
			if msgType != websocket.TextMessage {
				log.Println("[ERR] messageType must be websocket.TextMessage")
				continue
			}

			var m WebSocketMessage
			err = json.Unmarshal(msg, &m)
			if err != nil {
				log.Println("[ERR] json.Unmarshal", err)
				continue
			}
			switch m.Operation {
			case Subscribe:
				if _, exists := subscriptions[m.Key]; !exists {
					c, err := h.handleSubscribe(m, conn)
					if err != nil {
						log.Println("[ERR] h.handleSubscribe", err)
						continue
					}
					subscriptions[m.Key] = c
				}
			case Set:
				d, err := h.thunder.Store.Document(m.Key)
				if err != nil {
					log.Println("[ERR:Set]", err)
				}
				err = d.Set(m.Payload)
				if err != nil {
					log.Println("[ERR:Set]", err)
				}
			case Update:
				d, err := h.thunder.Store.Document(m.Key)
				if err != nil {
					log.Println("[ERR:Update]", err)
				}
				err = d.Update(m.Payload)
				if err != nil {
					log.Println("[ERR:Update]", err)
				}
			case Delete:
				d, err := h.thunder.Store.Document(m.Key)
				if err != nil {
					log.Println("[ERR:Delete]", err)
				}
				err = d.Delete()
				if err != nil {
					log.Println("[ERR:Delete]", err)
				}
			case Add:
				c, err := h.thunder.Store.Collection(m.Key)
				if err != nil {
					log.Println("[ERR:Add]", err)
				}
				_, err = c.Add(m.Payload)
				if err != nil {
					log.Println("[ERR:Add]", err)
				}
			}
		}
	}
}

func (h *WebSocketHandler) handleSubscribe(m WebSocketMessage, conn *websocket.Conn) (chan []byte, error) {
	var channel chan []byte
	var initialData []byte
	var err error

	if store.IsDocumentKey(m.Key) {
		channel = h.thunder.PubSub.Subscribe(m.Key)
		document, err := h.thunder.Store.Document(m.Key)
		if err != nil {
			return nil, err
		}
		initialData, err = document.Get()
		if err != nil {
			return nil, err
		}
	} else {
		queryFunc := func() ([]byte, error) {
			collection, err := h.thunder.Store.Collection(m.Key)
			if err != nil {
				return nil, err
			}
			p := m.OperationParameters
			items, err := collection.Items(p.Query, p.Order, p.Limit)
			if err != nil {
				return nil, err
			}
			data, err := json.Marshal(items);
			if err != nil {
				return nil, err
			}
			return data, nil
		}
		// Subscribe to the collection with the given function.
		channel = h.thunder.PubSub.SubscribeWithFunc(m.Key, queryFunc)
		initialData, err = queryFunc();
		if err != nil {
			return nil, err
		}
	}
	// Publish initial data snapshot..
	h.writeMessage(conn, &WebSocketMessage{
		Operation: ValueChange,
		Key:       m.Key,
		Payload:   initialData,
	})
	go h.listen(m.Key, channel, conn)
	return channel, err
}

func (h *WebSocketHandler) listen(key string, channel chan []byte, conn *websocket.Conn) {
	for {
		select {
		case m, ok := <-channel:
			if !ok {
				return
			}
			h.writeMessage(conn, &WebSocketMessage{
				Key:       key,
				Payload:   m,
				Operation: ValueChange,
			})
		}
	}
}

func (h *WebSocketHandler) writeMessage(conn *websocket.Conn, message *WebSocketMessage) {
	defer h.mutex.Unlock()
	h.mutex.Lock()

	data, err := json.Marshal(message)
	if err != nil {
		log.Println("[ERR] json.Marshal", err)
	}
	err = conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		log.Println("[ERR] websocket.Conn.WriteMessage", err)
	}
}
