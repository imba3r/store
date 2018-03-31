package thunder

import (
	"sort"
	"github.com/Jeffail/gabs"
)

func OrderBy(items []CollectionItem, path string, ascending bool) {
	sort.Slice(items, func(i, j int) bool {
		if !ascending {
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
		valueA := a.Search(path).Data()
		valueB := b.Search(path).Data()
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
