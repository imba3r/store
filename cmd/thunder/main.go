package main

import (
	"github.com/imba3r/thunder"
	"net/http"
	"log"

	"github.com/gorilla/websocket"
	"encoding/json"
)

type Operation int

const (
	Subscribe Operation = iota
	Insert
	Update
	Delete
	Snapshot
)

type Message struct {
	Key       string          `json:"key"`
	Operation Operation       `json:"operation,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

func main() {
	s := thunder.NewStore("/tmp/badger")

	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	http.HandleFunc("/thunder", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
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
				log.Println("[ERR] ReadMessage", err)
				return
			}
			if msgType != websocket.TextMessage {
				log.Println("[ERR] Invalid message type")
				continue
			}

			var m Message
			err = json.Unmarshal(msg, &m)
			if err != nil {
				log.Println("[ERR] Unmarshal", err)
				continue
			}

			switch m.Operation {
			case Subscribe:
				subscriptions[m.Key] = s.Subscribe(m.Key)
				go listen(m.Key, subscriptions[m.Key], conn)
			case Update:
				s.Update(m.Key, m.Payload)
			case Insert:
				s.Insert(m.Key, m.Payload)
			case Delete:
				s.Delete(m.Key)
			case Snapshot:
				data, _ := s.Read(m.Key)
				writeMessage(conn, createAnswer(m.Key, data))
			}
		}
	})
	http.ListenAndServe(":3000", nil)
}

func listen(key string, channel chan []byte, conn *websocket.Conn) {
	log.Println("Listening....")
	for {
		select {
		case m, ok := <-channel:
			if !ok {
				log.Println("Stopped listening..")
				return
			}
			writeMessage(conn, createAnswer(key, m))
		}
	}
}

func writeMessage(conn *websocket.Conn, message []byte) {
	err := conn.WriteMessage(websocket.TextMessage, message)
	if err != nil {
		log.Println("[ERR] listen().WriteMessage", err)
	}
}

func createAnswer(key string, data []byte) []byte {
	answer := &Message{
		Key:     key,
		Payload: data,
	}
	data, err := json.Marshal(answer)
	if err != nil {
		log.Println("[ERR] createAnswer().Marshal", err)
	}
	return data
}
