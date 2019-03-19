package mapper

import (
	"strconv"

	"github.com/ONSdigital/dp-code-list-api/models"
	"github.com/ONSdigital/dp-graph/graph/driver"
	"github.com/ONSdigital/golang-neo4j-bolt-driver/structures/graph"
)

//Codes returns a dpbolt.ResultMapper mapper which converts dpbolt.Result to models.CodeResults
func Codes(results *models.CodeResults, codeListID string, edition string) ResultMapper {
	return func(r *Result) error {
		code, err := code(r)
		if err != nil {
			return err
		}

		results.Items = append(results.Items, *code)
		return nil
	}
}

//Code returns a dpbolt.ResultMapper which converts a dpbolt.Result to models.Code
func Code(codeModel *models.Code, codeListID string, edition string) ResultMapper {
	return func(r *Result) error {
		co, err := code(r)
		if err != nil {
			return err
		}

		*codeModel = *co
		return nil
	}
}

func code(r *Result) (*models.Code, error) {
	if len(r.Data) == 0 {
		return nil, driver.ErrNotFound
	}

	var err error
	var node graph.Node
	if node, err = getNode(r.Data[0]); err != nil {
		return nil, err
	}

	id := node.NodeIdentity

	var codeVal string
	if codeVal, err = getStringProperty("value", node.Properties); err != nil {
		return nil, err
	}

	var rel graph.Relationship
	if rel, err = getRelationship(r.Data[1]); err != nil {
		return nil, err
	}

	var codeLabel string
	if codeLabel, err = getStringProperty("label", rel.Properties); err != nil {
		return nil, err
	}

	return &models.Code{
		ID:    strconv.FormatInt(id, 10),
		Code:  codeVal,
		Label: codeLabel,
		Links: &models.CodeLinks{
			Self: &models.Link{
				ID: codeVal,
			},
		},
	}, nil
}
