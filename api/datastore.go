package api

import (
	"github.com/ONSdigital/dp-dataset-api/models"
)

// DataStore provides a datastore.Backend interface used to store, retrieve, remove or update datasets
type DataStore struct {
	Backend Backend
}

//go:generate moq -out datastoretest/datastore.go -pkg backendtest . Backend

// Backend represents basic data access via Get, Remove and Upsert methods.
type Backend interface {
	GetDatasets() (*models.DatasetResults, error)
	GetDataset(id string) (*models.Dataset, error)
	GetEditions(id string) (*models.EditionResults, error)
	GetEdition(datasetID, editionID string) (*models.Edition, error)
	GetVersions(datasetID, editionID string) (*models.VersionResults, error)
	GetVersion(datasetID, editionID, versionID string) (*models.Version, error)
	UpsertDataset(id string, update interface{}) error
	UpsertEdition(id string, update interface{}) error
	UpsertVersion(id string, update interface{}) error
	UpsertContact(id string, update interface{}) error
}
