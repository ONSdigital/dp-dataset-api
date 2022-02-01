package models

import (
	"context"
	"github.com/ONSdigital/dp-dataset-api/contract"
)

// CantabularDataProvider represents all the required methods to access data from Cantabular
type CantabularDataProvider interface {
	PopulationTypes(ctx context.Context) ([]contract.PopulationType, error)
}
