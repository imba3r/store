package query

import (
	"github.com/imba3r/thunder/store"
	"github.com/Jeffail/gabs"
)

func Matches(item store.CollectionItem, query store.Query) bool {
	j, err := gabs.ParseJSON(item.Value)
	if err != nil {
		return false
	}
}