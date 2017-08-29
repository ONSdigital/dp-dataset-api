package api

import (
	"github.com/ONSdigital/dp-dataset-api/datastore"
	"github.com/ONSdigital/dp-dataset-api/models"
)


//go:generate moq -out datastoretest/datastore.go -pkg datastoretest . DataStore

// DataStore represents an interface used to store datasets
type DataStore interface {
	GetDatasets() (*models.DatasetResults, error)
	GetDataset(id string) (*models.Dataset, error)
	GetEditions(id string) (*models.EditionResults, error)
	GetEdition(datasetID, editionID string) (*models.Edition, error)
	GetVersions(datasetID, editionID string) (*models.VersionResults, error)
	GetVersion(datasetID, editionID, versionID string) (*models.Version, error)
	UpsertDataset(id interface{}, update interface{}) error
	UpsertEdition(id interface{}, update interface{}) error
	UpsertVersion(id interface{}, update interface{}) error
	UpsertContact(id interface{}, update interface{}) error
}

// DataStore provides a datastore.Backend interface used to store, retrieve, remove or update datasets
type DataStore1 struct {
	Backend datastore.Backend

}
