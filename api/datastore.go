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
	GetDataset(id string) (*models.DatasetUpdate, error)
	GetEditions(id string, selector interface{}) (*models.EditionResults, error)
	GetEdition(selector interface{}) (*models.Edition, error)
	GetNextVersion(datasetID, editionID string) (int, error)
	GetVersions(selector interface{}) (*models.VersionResults, error)
	GetVersion(selector interface{}) (*models.Version, error)
	UpdateDataset(id string, update interface{}) error
	UpdateEdition(id string, update interface{}) error
	UpsertDataset(id string, update interface{}) error
	UpsertEdition(id string, update interface{}) error
	UpsertVersion(id string, update interface{}) error
	UpsertContact(id string, update interface{}) error
}
