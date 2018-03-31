package thunder

import (
	"sort"
	"github.com/Jeffail/gabs"
)

type Order string

const (
	ASCENDING  Order = "ASC"
	DESCENDING Order = "DESC"
)

type OrderDataType string

const (
	Number OrderDataType = "NUMBER"
)

func OrderByNumber(items []CollectionItem, path string, ascending bool) {
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
