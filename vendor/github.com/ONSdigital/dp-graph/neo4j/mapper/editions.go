package mapper

import (
	"github.com/ONSdigital/dp-code-list-api/models"
	"github.com/ONSdigital/golang-neo4j-bolt-driver/structures/graph"
)

func Editions(editions *models.Editions) ResultMapper {
	return func(r *Result) error {
		edition, err := edition(r)
		if err != nil {
			return err
		}

		editions.Items = append(editions.Items, *edition)
		return nil
	}
}

func Edition(editionModel *models.Edition) ResultMapper {
	return func(r *Result) error {
		ed, err := edition(r)
		if err != nil {
			return err
		}

		*editionModel = *ed
		return nil
	}
}

func edition(r *Result) (*models.Edition, error) {
	var err error
	var node graph.Node

	node, err = getNode(r.Data[0])
	if err != nil {
		return nil, err
	}

	var edition string
	edition, err = getStringProperty("edition", node.Properties)
	if err != nil {
		return nil, err
	}

	var label string
	label, err = getStringProperty("label", node.Properties)
	if err != nil {
		return nil, err
	}

	return &models.Edition{
		Edition: edition,
		Label:   label,
		Links: &models.EditionLinks{
			Self: &models.Link{
				ID: edition,
			},
		},
	}, nil
}
