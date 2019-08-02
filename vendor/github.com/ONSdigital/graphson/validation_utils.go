package graphson

import (
	"fmt"
)

func EdgesMatch(edge1, edge2 Edge) (bool, string) {
	if edge1.Type != edge2.Type {
		return false, "type"
	}
	// if ok, reason := GenericValuesMatch(edge1.Value.ID, edge2.Value.ID); !ok {
	if edge1.Value.ID != edge2.Value.ID {
		return false, "id" // + reason
	}
	if edge1.Value.Label != edge2.Value.Label {
		return false, "label"
	}
	// if ok, reason := GenericValuesMatch(edge1.Value.InV, edge2.Value.InV); !ok {
	if edge1.Value.InV != edge2.Value.InV {
		return false, "inv" // + reason
	}
	if edge1.Value.InVLabel != edge2.Value.InVLabel {
		return false, "invlabel"
	}
	// if ok, reason := GenericValuesMatch(edge1.Value.OutV, edge2.Value.OutV); !ok {
	if edge1.Value.OutV != edge2.Value.OutV {
		return false, "outv" // + reason
	}
	if edge1.Value.OutVLabel != edge2.Value.OutVLabel {
		return false, "outvlabel"
	}
	if len(edge1.Value.Properties) != len(edge2.Value.Properties) {
		return false, "properties"
	}
	for label, edge1Props := range edge1.Value.Properties {
		edge2Props := edge2.Value.Properties[label]
		if edge1Props.Type != edge2Props.Type {
			return false, "prop.type"
		}
		if edge1Props.Value.Label != edge2Props.Value.Label ||
			fmt.Sprintf("%v", edge1Props.Value.Label) != fmt.Sprintf("%v", edge2Props.Value.Label) {
			return false, "prop.value"
		}
	}
	return true, ""
}

func VerticesMatch(vertex1, vertex2 Vertex) bool {
	if vertex1.Type != vertex2.Type {
		return false
	}
	if vertex1.Value.ID != vertex2.Value.ID {
		return false
	}
	if vertex1.Value.Label != vertex2.Value.Label {
		return false
	}
	if len(vertex1.Value.Properties) != len(vertex2.Value.Properties) {
		return false
	}
	for label, vertex1Props := range vertex1.Value.Properties {
		vertex2Props := vertex2.Value.Properties[label]
		if len(vertex1Props) != len(vertex2Props) {
			return false

		}
		for i, vertex1PropsElement := range vertex1Props {
			vertex2PropsElement := vertex2Props[i]
			if vertex1PropsElement.Type != vertex2PropsElement.Type {
				return false
			}
			if vertex1PropsElement.Value.ID.Type != vertex2PropsElement.Value.ID.Type ||
				fmt.Sprintf("%v", vertex1PropsElement.Value.ID.Value) != fmt.Sprintf("%v", vertex2PropsElement.Value.ID.Value) {
				return false
			}
			if vertex1PropsElement.Value.Label != vertex2PropsElement.Value.Label {
				return false
			}
			if fmt.Sprintf("%v", vertex1PropsElement.Value.Value) != fmt.Sprintf("%v", vertex2PropsElement.Value.Value) {
				return false
			}
		}
	}
	return true
}

func GenericValuesMatch(gv1, gv2 GenericValue) (bool, string) {
	if gv1.Type != gv2.Type {
		return false, "type"
	}
	gv1ValueString := fmt.Sprintf("%v", gv1.Value)
	gv2ValueString := fmt.Sprintf("%v", gv2.Value)
	return gv1ValueString == gv2ValueString, "value"
}
