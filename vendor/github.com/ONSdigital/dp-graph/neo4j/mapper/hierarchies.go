package mapper

import (
	"errors"

	"github.com/ONSdigital/dp-hierarchy-api/models"
	"github.com/ONSdigital/golang-neo4j-bolt-driver/structures/graph"
)

// HierarchyElements encases a list so a pointer to the list can more easily
// be passed into the mapper functions
type HierarchyElements struct {
	List []*models.Element
}

// HierarchyCodelist returns a dpbolt.ResultMapper which converts a dpbolt.Result to CodelistID string
func HierarchyCodelist(codelistID *string) ResultMapper {
	return func(r *Result) error {
		var node graph.Node
		var err error

		if node, err = getNode(r.Data[0]); err != nil {
			return err
		}

		id, err := getStringProperty("code_list", node.Properties)
		if err != nil {
			return errors.New("code_list property not found")
		}

		*codelistID = id

		return nil
	}
}

// Hierarchy returns a dpbolt.ResultMapper mapper which converts dpbolt.Result to models.Response
func Hierarchy(res *models.Response) ResultMapper {
	return func(r *Result) error {
		var node graph.Node
		var err error

		if node, err = getNode(r.Data[0]); err != nil {
			return err
		}

		var e *models.Element
		if e, err = createElement(node); err != nil {
			return err
		}

		res.ID = e.ID
		res.Label = e.Label
		res.HasData = e.HasData
		res.NoOfChildren = e.NoOfChildren

		return nil

	}
}

// HierarchyElement returns a dpbolt.ResultMapper mapper which converts dpbolt.Result to HierarchyElements
func HierarchyElement(list *HierarchyElements) ResultMapper {
	return func(r *Result) error {
		var node graph.Node
		var err error

		if node, err = getNode(r.Data[0]); err != nil {
			return err
		}

		var e *models.Element
		if e, err = createElement(node); err != nil {
			return err
		}

		list.List = append(list.List, e)
		return nil

	}
}

func createElement(node graph.Node) (*models.Element, error) {
	id, err := getStringProperty("code", node.Properties)
	if err != nil {
		return nil, errors.New("code property not found")
	}

	label, err := getStringProperty("label", node.Properties)
	if err != nil {
		return nil, errors.New("label property not found")
	}

	hasData, err := getBoolProperty("hasData", node.Properties)
	if err != nil {
		return nil, errors.New("hasData property not found")
	}

	children, err := getint64Property("numberOfChildren", node.Properties)
	if err != nil {
		return nil, errors.New("numberOfChildren property not found")
	}

	return &models.Element{
		ID:           id,
		Label:        label,
		HasData:      hasData,
		NoOfChildren: children,
	}, nil
}
