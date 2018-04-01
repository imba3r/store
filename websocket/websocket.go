package websocket

import (
	"encoding/json"
	"net/http"
	"log"

	"github.com/gorilla/websocket"
	"sync"
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
	OrderBy   string `json:"orderBy"`
	Ascending bool   `json:"ascending"`
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
				return
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
					subscriptions[m.Key] = h.thunder.PubSub.SubscribeWithFunc(m.Key, func() []byte {
						if store.IsDocumentKey(m.Key) {
							doc, _ := h.thunder.Store.Document(m.Key)
							data, _ := doc.Get();
							return data;
						}
						if store.IsCollectionKey(m.Key) {
							c, err := h.thunder.Store.Collection(m.Key)
							if err != nil {
								log.Println(err)
							}

							items, err := c.Items(store.Query{}, store.Order{}, store.Limit{})
							log.Println(items)
							if err != nil {
								log.Println(err)
							}

							data, err := json.Marshal(items);
							if err != nil {
								log.Println(err)
							}
							log.Println(string(data))
							return data
						}
						return nil
					})
					go h.listen(m.Key, subscriptions[m.Key], conn)
					// TODO: Distinguish documents and collections, send one snapshot here
					if store.IsDocumentKey(m.Key) {
						d, err := h.thunder.Store.Document(m.Key)
						if err != nil {
							log.Println("[ERR:Subscribe]", err)
						}
						data, err := d.Get()
						if err != nil {
							log.Println("[ERR:Subscribe]", err)
						}
						h.writeMessage(conn, createAnswer(ValueChange, m.Key, 0, data))
					}
					if store.IsCollectionKey(m.Key) {
						c, err := h.thunder.Store.Collection(m.Key)
						if err != nil {
							log.Println("[ERR:Subscribe]", err)
						}
						items, err := c.Items(store.Query{}, store.Order{}, store.Limit{})
						if err != nil {
							log.Println("[ERR:Subscribe]", err)
						}
						data, err := json.Marshal(items)
						if err != nil {
							log.Println("[ERR:Subscribe]", err)
						}
						h.writeMessage(conn, createAnswer(ValueChange, m.Key, 0, data))
					}
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

func (h *WebSocketHandler) listen(key string, channel chan []byte, conn *websocket.Conn) {
	for {
		select {
		case m, ok := <-channel:
			if !ok {
				return
			}
			h.writeMessage(conn, createAnswer(ValueChange, key, 0, m))
		}
	}
}

func (h *WebSocketHandler) writeMessage(conn *websocket.Conn, message []byte) {
	defer h.mutex.Unlock()
	h.mutex.Lock()

	err := conn.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		log.Println("[ERR] websocket.Conn.WriteMessage", err)
	}
}

func createAnswer(operation WebSocketOperation, key string, id uint64, data []byte) []byte {
	answer := &WebSocketMessage{
		Operation: operation,
		Key:       key,
		RequestID: id,
		Payload:   data,
	}
	data, err := json.Marshal(answer)
	if err != nil {
		log.Println("[ERR] json.Marshal", err)
	}
	return data
}
