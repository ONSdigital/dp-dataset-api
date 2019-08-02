package graphson

import (
	"errors"
	"strings"
)

var (
	ErrorPropertyNotFound       = errors.New("property not found")
	ErrorPropertyIsMeta         = errors.New("meta-property found where multi-property expected")
	ErrorPropertyIsMulti        = errors.New("multi-property found where singleton expected")
	ErrorUnexpectedPropertyType = errors.New("property value could not be cast into expected type")
)

// GetID returns the string ID for the given vertex
func (v Vertex) GetID() string {
	return v.Value.ID
}

// GetLabels returns the []string labels for the given vertex
func (v Vertex) GetLabels() (labels []string) {
	labels = append(labels, v.Value.Label)
	if strings.Index(labels[0], "::") == -1 {
		return
	}
	return strings.Split(labels[0], "::")
}

// GetLabel returns the string label for the given vertex, or an error if >1
func (v Vertex) GetLabel() (string, error) {
	labels := v.GetLabels()
	if len(labels) > 1 {
		return "", errors.New("too many labels - expected one")
	}
	return labels[0], nil
}

// GetMultiProperty returns the ([]string) values for the given property `key`
// will return an error if the property is not the correct type
func (v Vertex) GetMultiProperty(key string) (vals []string, err error) {
	var valsInterface []interface{}
	if valsInterface, err = v.GetMultiPropertyAs(key, "string"); err != nil {
		return
	}
	for _, val := range valsInterface {
		vals = append(vals, val.(string))
	}
	return
}

// GetMultiPropertyBool returns the ([]bool) values for the given property `key`
// will return an error if the property is not the correct type
func (v Vertex) GetMultiPropertyBool(key string) (vals []bool, err error) {
	var valsInterface []interface{}
	if valsInterface, err = v.GetMultiPropertyAs(key, "bool"); err != nil {
		return
	}
	for _, val := range valsInterface {
		vals = append(vals, val.(bool))
	}
	return
}

// GetMultiPropertyInt64 returns the ([]int64) values for the given property `key`
// will return an error if the property is not the correct type
func (v Vertex) GetMultiPropertyInt64(key string) (vals []int64, err error) {
	var valsInterface []interface{}
	if valsInterface, err = v.GetMultiPropertyAs(key, "int64"); err != nil {
		return
	}
	for _, val := range valsInterface {
		vals = append(vals, val.(int64))
	}
	return
}

// GetMultiPropertyInt32 returns the ([]int32) values for the given property `key`
// will return an error if the property is not the correct type
func (v Vertex) GetMultiPropertyInt32(key string) (vals []int32, err error) {
	var valsInterface []interface{}
	if valsInterface, err = v.GetMultiPropertyAs(key, "int32"); err != nil {
		return
	}
	for _, val := range valsInterface {
		vals = append(vals, val.(int32))
	}
	return
}

// GetMultiPropertyAs returns the values for the given property `key` as type `wantType`
// will return an error if the property is not a set of the given `wantType` (string, bool, int64)
func (v Vertex) GetMultiPropertyAs(key, wantType string) (vals []interface{}, err error) {
	var valInterface []VertexProperty
	var ok bool
	if valInterface, ok = v.Value.Properties[key]; !ok {
		err = ErrorPropertyNotFound
		return
	}
	for _, prop := range valInterface {
		if prop.Value.Label != key {
			err = ErrorPropertyIsMulti
			return
		}
		switch wantType {

		case "string":
			var val string
			if val, ok = prop.Value.Value.(string); !ok {
				err = ErrorUnexpectedPropertyType
				return
			}
			vals = append(vals, val)
		case "bool":
			var val bool
			if val, ok = prop.Value.Value.(bool); !ok {
				err = ErrorUnexpectedPropertyType
				return
			}
			vals = append(vals, val)
		case "int32":
			var typeIf, valIf interface{}
			if typeIf, ok = prop.Value.Value.(map[string]interface{})["@type"]; !ok || typeIf != "g:Int32" {
				return vals, ErrorUnexpectedPropertyType
			}
			if valIf, ok = prop.Value.Value.(map[string]interface{})["@value"]; !ok {
				return vals, ErrorUnexpectedPropertyType
			}
			var val float64
			if val, ok = valIf.(float64); !ok {
				return vals, ErrorUnexpectedPropertyType
			}
			vals = append(vals, int32(val))
		case "int64":
			typedPropValue := prop.Value.Value.(map[string]interface{})
			typeAsString, ok := typedPropValue["@type"]
			if !ok || (typeAsString != "g:Int64" && typeAsString != "g:Int32") {
				return vals, ErrorUnexpectedPropertyType
			}
			var valIf interface{}
			if valIf, ok = prop.Value.Value.(map[string]interface{})["@value"]; !ok {
				return vals, ErrorUnexpectedPropertyType
			}
			var val float64
			if val, ok = valIf.(float64); !ok {
				return vals, ErrorUnexpectedPropertyType
			}
			vals = append(vals, int64(val))
		}
	}
	return
}

// GetProperty returns the single string value for a given property `key`
// will return an error if the property is not a single string
func (v Vertex) GetProperty(key string) (val string, err error) {
	var vals []string
	if vals, err = v.GetMultiProperty(key); err != nil {
		return
	}
	if len(vals) == 0 {
		err = ErrorPropertyNotFound
		return
	}
	if len(vals) > 1 {
		err = ErrorPropertyIsMulti
		return
	}
	return vals[0], nil
}

// GetPropertyInt64 returns the single int64 value for a given property `key`
// will return an error if the property is not a single string
func (v Vertex) GetPropertyInt64(key string) (val int64, err error) {
	var valsInterface []interface{}
	if valsInterface, err = v.GetMultiPropertyAs(key, "int64"); err != nil {
		return
	}
	if len(valsInterface) == 0 {
		err = ErrorPropertyNotFound
		return
	}
	if len(valsInterface) > 1 {
		err = ErrorPropertyIsMulti
		return
	}
	return valsInterface[0].(int64), nil
}

// GetPropertyInt32 returns the single int32 value for a given property `key`
// will return an error if the property is not a single string
func (v Vertex) GetPropertyInt32(key string) (val int32, err error) {
	var valsInterface []interface{}
	if valsInterface, err = v.GetMultiPropertyAs(key, "int32"); err != nil {
		return
	}
	if len(valsInterface) == 0 {
		err = ErrorPropertyNotFound
		return
	}
	if len(valsInterface) > 1 {
		err = ErrorPropertyIsMulti
		return
	}
	return valsInterface[0].(int32), nil
}

// GetPropertyBool returns the single bool value for a given property `key`
// will return an error if the property is not a single string
func (v Vertex) GetPropertyBool(key string) (val bool, err error) {
	var valsInterface []interface{}
	if valsInterface, err = v.GetMultiPropertyAs(key, "bool"); err != nil {
		return
	}
	if len(valsInterface) == 0 {
		err = ErrorPropertyNotFound
		return
	}
	if len(valsInterface) > 1 {
		err = ErrorPropertyIsMulti
		return
	}
	return valsInterface[0].(bool), nil
}

// GetMetaProperty returns a map[string]string for the given property `key`
func (v Vertex) GetMetaProperty(key string) (metaMap map[string][]string, err error) {
	var valInterface []VertexProperty
	var ok bool
	if valInterface, ok = v.Value.Properties[key]; !ok {
		err = ErrorPropertyNotFound
		return
	}
	for _, prop := range valInterface {
		subKey := prop.Value.Label
		var subVal string
		if subVal, ok = prop.Value.Value.(string); !ok {
			err = ErrorUnexpectedPropertyType
			return
		}
		if metaMap == nil {
			metaMap = make(map[string][]string)
		}
		metaMap[subKey] = append(metaMap[subKey], subVal)
	}
	return
}
