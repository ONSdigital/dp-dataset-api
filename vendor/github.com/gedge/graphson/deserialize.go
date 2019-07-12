package graphson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

func DeserializeVertices(rawResponse string) ([]Vertex, error) {
	// TODO: empty strings for property values will cause invalid json
	// make so it can handle that case
	if len(rawResponse) == 0 {
		return []Vertex{}, nil
	}
	return DeserializeVerticesFromBytes([]byte(rawResponse))
}

func DeserializeVerticesFromBytes(rawResponse []byte) ([]Vertex, error) {
	// TODO: empty strings for property values will cause invalid json
	// make so it can handle that case
	var response []Vertex
	if len(rawResponse) == 0 {
		return response, nil
	}
	dec := json.NewDecoder(bytes.NewReader(rawResponse))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&response); err != nil {
		return nil, err
	}
	return response, nil
}

func DeserializeListOfVerticesFromBytes(rawResponse []byte) ([]Vertex, error) {
	var metaResponse ListVertices
	var response []Vertex
	if len(rawResponse) == 0 {
		return response, nil
	}
	dec := json.NewDecoder(bytes.NewReader(rawResponse))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&metaResponse); err != nil {
		return nil, err
	}

	if metaResponse.Type != "g:List" {
		return response, errors.New("DeserializeListOfVerticesFromBytes: Expected `g:List` type")
	}

	return metaResponse.Value, nil
}

func DeserializeListOfEdgesFromBytes(rawResponse []byte) (Edges, error) {
	var metaResponse ListEdges
	var response Edges
	if len(rawResponse) == 0 {
		return response, nil
	}
	dec := json.NewDecoder(bytes.NewReader(rawResponse))
	dec.DisallowUnknownFields()
	err := dec.Decode(&metaResponse)
	if err != nil {
		return nil, err
	}

	if metaResponse.Type != "g:List" {
		return response, errors.New("DeserializeListOfEdgesFromBytes: Expected `g:List` type")
	}

	return metaResponse.Value, nil
}

func DeserializeMapFromBytes(rawResponse []byte) (resMap map[string]interface{}, err error) {
	var metaResponse GList
	if len(rawResponse) == 0 {
		return
	}
	dec := json.NewDecoder(bytes.NewReader(rawResponse))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&metaResponse); err != nil {
		return nil, err
	}

	if metaResponse.Type != "g:Map" {
		return resMap, errors.New("DeserializeMapFromBytes: Expected `g:Map` type")
	}

	return resMap, nil
}

// DeserializePropertiesFromBytes is for converting vertex .properties() results into a map
func DeserializePropertiesFromBytes(rawResponse []byte, resMap map[string][]interface{}) (err error) {
	var metaResponse GList
	if len(rawResponse) == 0 {
		return
	}
	dec := json.NewDecoder(bytes.NewReader(rawResponse))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&metaResponse); err != nil {
		return
	}

	if metaResponse.Type != "g:List" {
		return errors.New("DeserializePropertiesFromBytes: Expected `g:List` type")
	}
	var props []VertexProperty
	if err = json.Unmarshal(metaResponse.Value, &props); err != nil {
		return
	}

	for _, prop := range props {
		if _, ok := resMap[prop.Value.Label]; !ok {
			resMap[prop.Value.Label] = []interface{}{prop.Value.Value}
		} else {
			resMap[prop.Value.Label] = append(resMap[prop.Value.Label], prop.Value.Value)
		}
	}

	return
}

// DeserializeStringListFromBytes get a g:List value which should be a a list of strings, return those
func DeserializeStringListFromBytes(rawResponse []byte) (vals []string, err error) {
	var metaResponse GList
	if len(rawResponse) == 0 {
		err = errors.New("DeserializeStringListFromBytes: nothing to decode")
		return
	}
	dec := json.NewDecoder(bytes.NewReader(rawResponse))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&metaResponse); err != nil {
		return
	}

	if metaResponse.Type != "g:List" {
		err = errors.New("DeserializeStringListFromBytes: Expected `g:List` type")
		return
	}

	if err = json.Unmarshal(metaResponse.Value, &vals); err != nil {
		return
	}
	return
}

// DeserializeSingleFromBytes get a g:List value which should be a singular item, returns that item
func DeserializeSingleFromBytes(rawResponse []byte) (gV GenericValue, err error) {
	var metaResponse GList
	if len(rawResponse) == 0 {
		err = errors.New("DeserializeSingleFromBytes: nothing to decode")
		return
	}
	dec := json.NewDecoder(bytes.NewReader(rawResponse))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&metaResponse); err != nil {
		return
	}

	if metaResponse.Type != "g:List" {
		err = errors.New("DeserializeSingleFromBytes: Expected `g:List` type")
		return
	}

	var genVals GenericValues
	if genVals, err = DeserializeGenericValues(string(metaResponse.Value)); err != nil {
		return
	}

	if len(genVals) != 1 {
		err = fmt.Errorf("DeserializeSingleFromBytes: Expected single value, got %d", len(genVals))
		return
	}

	return genVals[0], nil
}

// DeserializeNumber returns the count from the g:List'd database response
func DeserializeNumber(rawResponse []byte) (count int64, err error) {
	var genVal GenericValue
	if genVal, err = DeserializeSingleFromBytes(rawResponse); err != nil {
		return
	}

	if genVal.Type != "g:Int64" {
		err = errors.New("DeserializeNumber: Expected `g:Int64` type")
		return
	}
	count = int64(genVal.Value.(float64))
	return
}

func DeserializeEdges(rawResponse string) (Edges, error) {
	var response Edges
	if rawResponse == "" {
		return response, nil
	}
	err := json.Unmarshal([]byte(rawResponse), &response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func DeserializeGenericValue(rawResponse string) (response GenericValue, err error) {
	if len(rawResponse) == 0 {
		return
	}
	if err = json.Unmarshal([]byte(rawResponse), &response); err != nil {
		return
	}
	return
}

func DeserializeGenericValues(rawResponse string) (GenericValues, error) {
	var response GenericValues
	if rawResponse == "" {
		return response, nil
	}
	err := json.Unmarshal([]byte(rawResponse), &response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func ConvertToCleanVertices(vertices []Vertex) []CleanVertex {
	var responseVertices []CleanVertex
	for _, vertex := range vertices {
		responseVertices = append(responseVertices, CleanVertex{
			Id:    vertex.Value.ID,
			Label: vertex.Value.Label,
		})
	}
	return responseVertices
}

func ConvertToCleanEdges(edges Edges) []CleanEdge {
	var responseEdges []CleanEdge
	for _, edge := range edges {
		responseEdges = append(responseEdges, CleanEdge{
			Source: edge.Value.InV,
			Target: edge.Value.OutV,
		})
	}
	return responseEdges
}
