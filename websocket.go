package thunder

import (
	"encoding/json"
	"net/http"
	"log"

	"github.com/gorilla/websocket"
)

type WebSocketOperation string

const (
	// Incoming
	Subscribe   WebSocketOperation = "SUBSCRIBE"
	Add         WebSocketOperation = "ADD"
	Set         WebSocketOperation = "SET"
	Update      WebSocketOperation = "UPDATE"
	Delete      WebSocketOperation = "DELETE"

	// Outgoing
	ValueChange WebSocketOperation = "VALUE_CHANGE"

	// Incoming & Outgoing
	Snapshot    WebSocketOperation = "SNAPSHOT"
)

type WebSocketMessage struct {
	Operation       WebSocketOperation `json:"operation"`
	Key             string             `json:"key"`
	ID              uint64             `json:"id"`
	Payload         json.RawMessage    `json:"payload,omitempty"`
	PayloadMetadata PayloadMetadata    `json:"payloadMetadata,omitempty"`
}

type PayloadMetadata struct {
	Type   ValueType `json:"valueType"`
	Exists bool      `json:"exists"`
}

func (s *Store) HandlerFunc() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := s.upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("[ERR] websocket.Upgrader.Upgrade", err)
			return
		}

		// Create a map of subscriptions (mapped by key) to the underlying storage.
		// Unsubscribe from them in the connection's close handler.
		subscriptions := make(map[string]chan []byte)
		conn.SetCloseHandler(func(code int, text string) error {
			for key, channel := range subscriptions {
				s.Unsubscribe(key, channel)
			}
			return nil;
		})

		for {
			msgType, msg, err := conn.ReadMessage()
			log.Println(string(msg))
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
					subscriptions[m.Key] = s.Subscribe(m.Key)
					data, _ := s.Read(m.Key)
					s.writeMessage(conn, createAnswer(Snapshot, m.Key, m.ID, data))
					go s.listen(m.Key, subscriptions[m.Key], conn)
				}
			case Update:
				s.Update(m.Key, m.Payload)
			case Add:
				s.Insert(m.Key, m.Payload)
			case Delete:
				s.Delete(m.Key)
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
			s.writeMessage(conn, createAnswer(ValueChange, key, 0, m))
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

func createAnswer(operation WebSocketOperation, key string, id uint64, data []byte) []byte {
	answer := &WebSocketMessage{
		Operation: operation,
		Key:       key,
		ID:        id,
		Payload:   data,
	}
	data, err := json.Marshal(answer)
	if err != nil {
		log.Println("[ERR] json.Marshal", err)
	}
	return data
}
