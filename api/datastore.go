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
	GetEditions(id, state string) (*models.EditionResults, error)
	GetEdition(id, editionID, state string) (*models.Edition, error)
	GetNextVersion(datasetID, editionID string) (int, error)
	GetVersions(datasetID, editionID, state string) (*models.VersionResults, error)
	GetVersion(datasetID, editionID, version, state string) (*models.Version, error)
	UpdateDatasetWithAssociation(id, state string, version *models.Version) error
	UpdateEdition(id, state string) error
	UpsertDataset(id string, datasetDoc *models.DatasetUpdate) error
	UpsertEdition(id string, editionDoc *models.Edition) error
	UpsertVersion(id string, versionDoc *models.Version) error
	UpsertContact(id string, update interface{}) error
}
