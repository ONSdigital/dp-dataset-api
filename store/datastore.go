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
	AddDimensionToInstance(dimension *models.CachedDimension) error
	AddEventToInstance(instanceID string, event *models.Event) error
	AddInstance(instance *models.Instance) (*models.Instance, error)
	GetDataset(id string) (*models.DatasetUpdate, error)
	GetDatasets() (*models.DatasetResults, error)
	GetDimensionNodesFromInstance(id string) (*models.DimensionNodeResults, error)
	GetDimensions(datasetID, editionID, versionID string) (*models.DatasetDimensionResults, error)
	GetEdition(id, editionID, state string) (*models.Edition, error)
	GetEditions(id, state string) (*models.EditionResults, error)
	GetInstances(filter string) (*models.InstanceResults, error)
	GetInstance(id string) (*models.Instance, error)
	GetNextVersion(datasetID, editionID string) (int, error)
	GetUniqueDimensionValues(id, dimension string) (*models.DimensionValues, error)
	GetVersion(datasetID, editionID, version, state string) (*models.Version, error)
	GetVersions(datasetID, editionID, state string) (*models.VersionResults, error)
	UpsertContact(id string, update interface{}) error
	UpsertDataset(id string, datasetDoc *models.DatasetUpdate) error
	UpdateDatasetWithAssociation(id, state string, version *models.Version) error
	UpdateDimensionNodeID(dimension *models.CachedDimension) error
	UpdateEdition(id, state string) error
	UpsertEdition(id string, editionDoc *models.Edition) error
	UpdateInstance(id string, instance *models.Instance) error
	UpdateObservationInserted(id string, observationInserted int64) error
	UpsertVersion(id string, versionDoc *models.Version) error
}
