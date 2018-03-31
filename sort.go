package thunder

import (
	"sort"
	"github.com/Jeffail/gabs"
)

func sortByNumber(items []CollectionItem, path string) {
	sort.Slice(items, func(i, j int) bool {
		a, err := gabs.ParseJSON(items[i].Value)
		if err != nil {
			return true
		}
		b, err := gabs.ParseJSON(items[j].Value)
		if err != nil {
			return false
		}
		valueA, ok := a.Search(path).Data().(float64)
		if !ok {
			return true
		}
		valueB, ok := b.Search(path).Data().(float64)
		if !ok {
			return false
		}
		return valueA < valueB
	})
}
