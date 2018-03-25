package main

import (
	"fmt"
	"github.com/imba3r/thunder"
	"time"
)

func main() {
	s := thunder.NewStore("/tmp/badger")
	defer s.Close()

	c := s.Subscribe("key")
	go listen(c)

	time.Sleep(1 * time.Second)

	s.Insert("key", []byte("data"))
	s.Update("key", []byte("newdata"))
	s.Delete("key")

	s.Unsubscribe("key", c)

	time.Sleep(2 * time.Second)
}

func listen(c chan []byte) {
	for {
		select {
		case message, ok := <-c:
			if !ok {
				fmt.Println("Unsubscribed!")
				return
			}
			fmt.Println("Message received!", string(message))
		}
	}
}
