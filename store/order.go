package store

import (
	"sort"
	"github.com/Jeffail/gabs"
)

type Order struct {
	OrderBy   string `json:"orderBy"`
	Ascending bool   `json:"ascending"`
}

func OrderJSON(items []CollectionItem, order Order) {
	sort.Slice(items, func(i, j int) bool {
		if !order.Ascending {
			i, j = j, i
		}
		a, err := gabs.ParseJSON(items[i].Value)
		if err != nil {
			return true
		}
		b, err := gabs.ParseJSON(items[j].Value)
		if err != nil {
			return false
		}
		valueA := a.Path(order.OrderBy).Data()
		valueB := b.Path(order.OrderBy).Data()
		if valueA == nil && valueB != nil {
			return true
		}
		if valueB == nil {
			return false
		}
		return Less(valueA, valueB)
	})
}

func Less(a interface{}, b interface{}) bool {
	switch a.(type) {
	case float64:
		b, ok := b.(float64)
		return ok && a.(float64) < b
	case string:
		b, ok := b.(string)
		return ok && a.(string) < b
	default:
		return false
	}
}
