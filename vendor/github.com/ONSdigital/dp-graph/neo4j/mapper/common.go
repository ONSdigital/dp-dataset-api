package mapper

import (
	"reflect"
	"strconv"

	"github.com/ONSdigital/golang-neo4j-bolt-driver/structures/graph"
	"github.com/pkg/errors"
)

// ResultMapper defines the function signature all mappers will need to satisfy
type ResultMapper func(r *Result) error

// Result contains data and metadata relating to a neo4j query response
type Result struct {
	Data  []interface{}
	Meta  map[string]interface{}
	Index int
}

// ErrInputNil is used when a result was expected but was nil
var ErrInputNil = errors.New("expected input value but was nil")

// getNode return val as graph.Node if cast successful, otherwise return a detailed error.
func getNode(val interface{}) (graph.Node, error) {
	var graphNode graph.Node
	var ok bool

	if val == nil {
		return graphNode, ErrInputNil
	}

	graphNode, ok = val.(graph.Node)
	if !ok {
		return graphNode, castingError(graphNode, val)
	}
	return graphNode, nil
}

// getNode return val as graph.Relationship if cast successful, otherwise return a detailed error.
func getRelationship(val interface{}) (graph.Relationship, error) {
	var r graph.Relationship
	var ok bool

	if val == nil {
		return r, ErrInputNil
	}

	r, ok = val.(graph.Relationship)
	if !ok {
		return r, castingError(r, val)
	}
	return r, nil
}

// getStringProperty return requested key value from map as a string. If key not found returns empty string and nil,
// returns casting error if val cannot be cast to string.
func getStringProperty(key string, props map[string]interface{}) (string, error) {
	var strVal string
	var ok bool

	if props == nil {
		return strVal, ErrInputNil
	}

	val, ok := props[key]
	if !ok {
		return strVal, nil
	}

	strVal, ok = val.(string)
	if !ok {
		return strVal, castingError(strVal, val)
	}
	return strVal, nil
}

// getBoolProperty return requested key value from map as a string. If key not found returns empty string and nil,
// returns casting error if val cannot be cast to string.
func getBoolProperty(key string, props map[string]interface{}) (bool, error) {
	var boolVal bool
	var ok bool

	if props == nil {
		return boolVal, ErrInputNil
	}

	val, ok := props[key]
	if !ok {
		return boolVal, nil
	}

	boolVal, ok = val.(bool)
	if !ok {
		return boolVal, castingError(boolVal, val)
	}
	return boolVal, nil
}

// getint64Property return requested key value from map as a int64. If key not found returns empty 0 and nil,
// returns casting error if val cannot be cast to int64.
func getint64Property(key string, props map[string]interface{}) (int64, error) {
	val, ok := props[key]
	if !ok {
		return 0, nil
	}

	intVal, ok := val.(int64)
	if !ok {
		var expected int64
		return expected, castingError(expected, val)
	}
	return intVal, nil
}

func castingError(expected interface{}, actual interface{}) error {
	t1 := reflect.TypeOf(expected).String()
	t2 := reflect.TypeOf(actual).String()
	return errors.Errorf("failed to cast value to requested type, expected %q but was type %q", t1, t2)
}

// GetCount returns dpbolt.ResultMapper for extracting an int64 value from a dpbolt.Result
func GetCount() (*int64, ResultMapper) {
	var count int64
	return &count, func(r *Result) error {
		if len(r.Data) != 1 {
			return errors.Errorf("get count error: expecting single result value but %d returned", len(r.Data))
		}
		var ok bool
		count, ok = r.Data[0].(int64)
		if !ok {
			return castingError(int64(0), r.Data[0])
		}
		return nil
	}
}

// GetNodeID returns dpbolt.ResultMapper for extracting a node id from a dpbolt.Result
func GetNodeID(id *string) ResultMapper {
	return func(r *Result) error {
		nodeID, ok := r.Data[0].(int64)
		if !ok {
			return errors.New("unexpected error while casting node id to int64")
		}
		*id = strconv.FormatInt(nodeID, 10)
		return nil
	}
}
