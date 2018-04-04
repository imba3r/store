package store_test

import (
	"testing"
	"encoding/json"
	"github.com/imba3r/thunder/store"
)

func TestMatches_Float(t *testing.T) {
	test := struct{ Test float64 }{Test: 1234}
	data, _ := json.Marshal(test)

	matches := store.Matches(data, store.Query{
		Value:    "1234",
		Operator: store.Eq,
		Field:    "Test",
	})

	if !matches {
		t.Errorf("Expected test data to match...")
	}
}

func TestMatches_String(t *testing.T) {
	test := struct{ Test string }{Test: "1234"}
	data, _ := json.Marshal(test)

	matches := store.Matches(data, store.Query{
		Value:    "1234",
		Operator: store.Eq,
		Field:    "Test",
	})

	if !matches {
		t.Errorf("Expected test data to match...")
	}
}

func TestMatches_Nil(t *testing.T) {
	test := struct{}{}
	data, _ := json.Marshal(test)

	matches := store.Matches(data, store.Query{
		Value:    "",
		Operator: store.Eq,
		Field:    "Test",
	})

	if !matches {
		t.Errorf("Expected test data to match...")
	}
}
