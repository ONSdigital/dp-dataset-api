package store

import (
	"github.com/ONSdigital/dp-dataset-api/models"
)

// DataStore provides a datastore.Storer interface used to store, retrieve, remove or update datasets
type DataStore struct {
	Backend Storer
}

//go:generate moq -out datastoretest/datastore.go -pkg storetest . Storer

// Storer represents basic data access via Get, Remove and Upsert methods.
type Storer interface {
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

	GetInstances(filter string) (*models.InstanceResults, error)
	GetInstance(id string) (*models.Instance, error)
	UpdateInstance(id string, instance *models.Instance) error
	AddInstance(instance *models.Instance) (*models.Instance, error)
	AddEventToInstance(instanceID string, event *models.Event) error
	AddDimensionToInstance(dimension *models.Dimension) error
	UpdateObservationInserted(id string, observationInserted int64) error
	GetDimensionNodesFromInstance(id string) (*models.DimensionNodeResults, error)
	UpdateDimensionNodeID(dimension *models.Dimension) error
	GetUniqueDimensionValues(id, dimension string) (*models.DimensionValues, error)
}
