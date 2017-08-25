package api

import (
	"github.com/ONSdigital/dp-dataset-api/models"
)

//go:generate moq -out datastoretest/datastore.go -pkg datastoretest . DataStore

// DataStore represents an interface used to store datasets
type DataStore interface {
	GetAllDatasets() (*models.DatasetResults, error)
	GetDataset(id string) (*models.Dataset, error)
	GetEditions(id string) (*models.EditionResults, error)
	GetEdition(datasetID, editionID string) (*models.Edition, error)
}
