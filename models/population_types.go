package models

import (
	"context"
)

type PopulationTypes struct {
	Items []PopulationType `json:"items"`
}

func FetchPopulationTypes(ctx context.Context, cantabular CantabularDataProvider) (PopulationTypes, error) {
	items, err := cantabular.PopulationTypes(ctx)
	if err != nil {
		return PopulationTypes{}, err
	}
	blobs := PopulationTypes{items}
	return blobs, nil
}
