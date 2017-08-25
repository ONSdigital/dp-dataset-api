package api

import (
	"github.com/ONSdigital/dp-dataset-api/datastore"
	"github.com/ONSdigital/dp-dataset-api/models"
)


//go:generate moq -out datastoretest/datastore.go -pkg datastoretest . DataStore

// DataStore represents an interface used to store datasets
type DataStore interface {
	GetAllDatasets() (*models.DatasetResults, error)
	GetDataset(id string) (*models.Dataset, error)
	GetEditions(id string) (*models.EditionResults, error)
	GetEdition(datasetID, editionID string) (*models.Edition, error)
	GetVersions(datasetID, editionID string) (*models.VersionResults, error)
	GetVersion(datasetID, editionID, versionID string) (*models.Version, error)
}

// DataStore provides a datastore.Backend interface used to store, retrieve, remove or update datasets
type DataStore1 struct {
	Backend datastore.Backend

}
