package internal

import (
	"fmt"

	"github.com/gedge/graphson"
)

/*
This module provides a handful of mock convenience functions that can be
used to inject behaviour into NeptunePoolMock.
*/

import (
	"errors"
)

// ReturnOne is a mock implementation for NeptunePool.GetCount()
// that always returns a count of 1.
var ReturnOne = func(q string, bindings, rebindings map[string]string) (i int64, err error) {
	return 1, nil
}

// ReturnTwo is a mock implementation for NeptunePool.GetCount()
// that always returns a count of 2.
var ReturnTwo = func(q string, bindings, rebindings map[string]string) (i int64, err error) {
	return 2, nil
}

// ReturnZero is a mock implementation for NeptunePool.GetCount()
// that always returns a count of 0.
var ReturnZero = func(q string, bindings, rebindings map[string]string) (i int64, err error) {
	return 0, nil
}

// ReturnMalformedIntRequestErr is a mock implementation for NeptunePool.GetCount()
// that always returns an error that is judged to be not transient by
// neptune.isTransientError
var ReturnMalformedIntRequestErr = func(q string, bindings, rebindings map[string]string) (i int64, err error) {
	return -1, errors.New(" MALFORMED REQUEST ")
}

// ReturnMalformedNilInterfaceRequestErr is a mock implementation for
// NeptunePool functions that return  (Interface{}, error) which always returns an
// error that is judged to be not transient by neptune.isTransientError
var ReturnMalformedNilInterfaceRequestErr = func(q string, bindings, rebindings map[string]string) (interface{}, error) {
	return nil, errors.New(" MALFORMED REQUEST ")
}

// ReturnMalformedStringListRequestErr is a mock implementation for
// NeptunePool functions that return  ([]string, error) which always returns an
// error that is judged to be not transient by neptune.isTransientError
var ReturnMalformedStringListRequestErr = func(q string, bindings, rebindings map[string]string) ([]string, error) {
	return nil, errors.New(" MALFORMED REQUEST ")
}

// ReturnThreeCodeLists is mock implementation for NeptunePool.Get() that always
// returns a slice of three graphson.Vertex(s):
// - of type "_code_list"
// - with a "listID" property set to "listID_0", "listID_1", and "ListID_2" respectively.
// - with an "edition" property set to "my-test-edition"
var ReturnThreeCodeLists = func(query string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
	codeLists := []graphson.Vertex{}
	for i := 0; i < 3; i++ {
		vertex := makeCodeListVertex(i, "my-test-edition")
		codeLists = append(codeLists, vertex)
	}
	return codeLists, nil
}

// ReturnThreeEditionVertices is mock implementation for NeptunePool.Get() that always
// returns a slice of three graphson.Vertex(s):
// - of type "unused-vertex-type"
// - with a an "edition" property set to "edition_0", "edition_1", and "edition_2" respectively.
var ReturnThreeEditionVertices = func(query string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
	editions := []graphson.Vertex{}
	for i := 0; i < 3; i++ {
		vertex := makeVertex("unused-vertex-type")
		setVertexStringProperty(&vertex, "edition", fmt.Sprintf("edition_%d", i))
		editions = append(editions, vertex)
	}
	return editions, nil
}

// ReturnThreeCodeVertices is mock implementation for NeptunePool.Get() that always
// returns a slice of three graphson.Vertex(s):
// - of type "unused-vertex-type"
// - with a "value" property set to "code_0", "code_1", and "code_2" respectively.
var ReturnThreeCodeVertices = func(query string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
	codes := []graphson.Vertex{}
	for i := 0; i < 3; i++ {
		vertex := makeVertex("unused-vertex-type")
		setVertexStringProperty(&vertex, "value", fmt.Sprintf("code_%d", i))
		codes = append(codes, vertex)
	}
	return codes, nil
}

// ReturnThreeUselessVertices is mock implementation for NeptunePool.Get() that always
// returns a slice of three graphson.Vertex(s) of type "_useless_vertex_type", and with
// no properties set.
var ReturnThreeUselessVertices = func(query string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
	codeLists := []graphson.Vertex{}
	for i := 0; i < 3; i++ {
		vertex := makeVertex("_useless_vertex_type")
		codeLists = append(codeLists, vertex)
	}
	return codeLists, nil
}

// ReturnZeroVertices provides an empty list of graphson.Vertex(s)
var ReturnZeroVertices = func(query string, bindings map[string]string, rebindings map[string]string) (interface{}, error) {
	return []graphson.Vertex{}, nil
}

/*
makeVertex makes a graphson.Vertex of a given type (e.g. "_code_list").
*/
func makeVertex(vertexType string) graphson.Vertex {
	vertexValue := graphson.VertexValue{
		ID:         "unused_vertex_value_ID",
		Label:      vertexType,
		Properties: map[string][]graphson.VertexProperty{},
	}
	vertex := graphson.Vertex{Type: vertexType, Value: vertexValue}
	return vertex
}

/*
setVertexTypedProperty sets the given key/polymorphic-value to a vertex.
The "theType" parameter must be "string" or "int".
*/
func setVertexTypedProperty(theType string, vertex *graphson.Vertex, key string, value interface{}) {
	gv := graphson.GenericValue{Type: "string", Value: key}
	pv := graphson.VertexPropertyValue{
		ID:    gv,
		Label: key,
		Value: value,
	}
	vertexProperty := graphson.VertexProperty{Type: theType, Value: pv}
	vertexProperties := []graphson.VertexProperty{vertexProperty}
	vertex.Value.Properties[key] = vertexProperties
}

// setVertexStringProperty sets the given key/value in a vertex.
func setVertexStringProperty(vertex *graphson.Vertex, key string, value interface{}) {
	setVertexTypedProperty("string", vertex, key, value)
}

// setVertexIntProperty sets the given key/value in a vertex.
func setVertexIntProperty(vertex *graphson.Vertex, key string, value int) {
	setVertexTypedProperty("int", vertex, key, value)
}

// makeCodeListVertex provides a graphson.Vertex with a vertex type of the
// form "_code_list", and a "listID" property of the form "listID_3".
// It is also given an "edition" property with the supplied value.
func makeCodeListVertex(listIDSuffix int, edition string) graphson.Vertex {
	v := makeVertex("_code_list")
	setVertexStringProperty(&v, "listID", fmt.Sprintf("listID_%d", listIDSuffix))
	setVertexStringProperty(&v, "edition", edition)
	return v
}

// ReturnFiveStrings is a mock implementation for
// NeptunePool functions that return  ([]string, error) which always returns
// five strings.
var ReturnFiveStrings = func(q string, bindings, rebindings map[string]string) ([]string, error) {
	return []string{"a", "b", "c", "d", "e"}, nil
}

// ReturnStringRecordWithNonIntegerFourthElement is a mock implementation for
// NeptunePool functions that return  ([]string, error) which always returns
// 4 strings - in which the third one cannot be cast to an integer.
var ReturnStringRecordWithNonIntegerFourthElement = func(q string, bindings, rebindings map[string]string) ([]string, error) {
	return []string{"1", "2", "fibble", "3"}, nil
}

// ReturnProperlyFormedDatasetRecord is a mock implementation for
// NeptunePool functions that return  ([]string, error) which always returns
// A single quartet of strings that should satisfy the GetCodeDatasets method.
var ReturnProperlyFormedDatasetRecord = func(q string, bindings, rebindings map[string]string) ([]string, error) {
	return []string{"exampleDimName", "exampleDatasetEdition", "3", "exampleDatasetID"}, nil
}
