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

	GetDimensions(datasetID, editionID, versionID string) (*models.DatasetDimensionResults, error)

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
