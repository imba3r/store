package main

import (
	"net/http"

	"github.com/imba3r/thunder"
	"github.com/imba3r/thunder/store/badger"
)

func main() {
	t := thunder.New(badger.New("/tmp/store"), true)
	h := thunder.NewWebSocketHandler(t)
	http.HandleFunc("/thunder", h.HandlerFunc())
	http.ListenAndServe(":3000", nil)
}
