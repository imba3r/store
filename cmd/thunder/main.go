package main

import (
	"net/http"

	"github.com/imba3r/thunder"
	"github.com/imba3r/thunder/store/badger"
)

func main() {
	t := thunder.New(badger.New("/tmp/badger"))
	http.HandleFunc("/thunder", t.HandlerFunc())
	http.ListenAndServe(":3000", nil)
}
