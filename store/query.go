package store

import (
	"strconv"

	"github.com/Jeffail/gabs"
)

type Operator string

const (
	Eq Operator = "=="
	Lt Operator = "<"
	Le Operator = "<="
	Gt Operator = ">"
	Ge Operator = ">="
)

type Query struct {
	Field    string   `json:"field"`
	Operator Operator `json:"operator"`
	Value    string   `json:"value"`
}

func MatchesJSON(data []byte, query Query) bool {
	j, err := gabs.ParseJSON(data)
	if err != nil {
		return false
	}
	field := j.Path(query.Field).Data()
	switch field.(type) {
	case nil:
		return query.Operator == Eq && query.Value == ""
	case string:
		return compareString(field.(string), query.Operator, query.Value)
	case float64:
		floatValue, err := strconv.ParseFloat(query.Value, 64)
		if err != nil {
			return false
		}
		return compareFloat(field.(float64), query.Operator, floatValue)
	}
	return false
}

func compareString(a string, operator Operator, b string) bool {
	switch operator {
	case Eq:
		return a == b
	case Ge:
		return a >= b
	case Gt:
		return a > b
	case Le:
		return a <= b
	case Lt:
		return a < b
	}
	panic("illegal operator")
}

func compareFloat(a float64, operator Operator, b float64) bool {
	switch operator {
	case Eq:
		return a == b
	case Ge:
		return a >= b
	case Gt:
		return a > b
	case Le:
		return a <= b
	case Lt:
		return a < b
	}
	panic("illegal operator")
}
