package main

import (
	"net/http"

	"github.com/imba3r/thunder"
)

func main() {
	s := thunder.NewStore("/tmp/badger")
	http.HandleFunc("/thunder", s.HandlerFunc())
	http.ListenAndServe(":3000", nil)
}
