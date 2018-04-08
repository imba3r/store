package main

import (
	"net/http"

	"github.com/imba3r/thunder"
	"github.com/imba3r/thunder/store/badger"
	"github.com/imba3r/thunder/websocket"
	"github.com/imba3r/thunder/store"
)

func main() {
	bs := badger.New("/tmp/store");

	t := thunder.New(bs, true)
	t.Open(store.Json)
	
	h := websocket.NewWebSocketHandler(t)
	http.HandleFunc("/thunder", h.HandlerFunc())
	http.ListenAndServe(":3000", nil)
}
