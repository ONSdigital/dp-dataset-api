package store

import (
	"context"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-graph/observation"
	"github.com/globalsign/mgo/bson"
)

// DataStore provides a datastore.Storer interface used to store, retrieve, remove or update datasets
type DataStore struct {
	Backend Storer
}

//go:generate moq -out datastoretest/datastore.go -pkg storetest . Storer

// Storer represents basic data access via Get, Remove and Upsert methods.
type Storer interface {
	AddDimensionToInstance(dimension *models.CachedDimensionOption) error
	AddEventToInstance(instanceID string, event *models.Event) error
	AddInstance(instance *models.Instance) (*models.Instance, error)
	CheckDatasetExists(ID, state string) error
	CheckEditionExists(ID, editionID, state string) error
	GetDataset(ID string) (*models.DatasetUpdate, error)
	GetDatasets(ctx context.Context) ([]models.DatasetUpdate, error)
	GetDimensionsFromInstance(ID string) (*models.DimensionNodeResults, error)
	GetDimensions(datasetID, versionID string) ([]bson.M, error)
	GetDimensionOptions(version *models.Version, dimension string) (*models.DimensionOptionResults, error)
	GetEdition(ID, editionID, state string) (*models.EditionUpdate, error)
	GetEditions(ctx context.Context, ID, state string) (*models.EditionUpdateResults, error)
	GetInstances(ctx context.Context, states []string, datasets []string) (*models.InstanceResults, error)
	GetInstance(ID string) (*models.Instance, error)
	GetNextVersion(datasetID, editionID string) (int, error)
	GetUniqueDimensionAndOptions(ID, dimension string) (*models.DimensionValues, error)
	GetVersion(datasetID, editionID, version, state string) (*models.Version, error)
	GetVersions(ctx context.Context, datasetID, editionID, state string) (*models.VersionResults, error)
	UpdateDataset(ctx context.Context, ID string, dataset *models.Dataset, currentState string) error
	UpdateDatasetWithAssociation(ID, state string, version *models.Version) error
	UpdateDimensionNodeID(dimension *models.DimensionOption) error
	UpdateInstance(ctx context.Context, ID string, instance *models.Instance) error
	UpdateObservationInserted(ID string, observationInserted int64) error
	UpdateImportObservationsTaskState(id, state string) error
	UpdateBuildHierarchyTaskState(id, dimension, state string) error
	UpdateBuildSearchTaskState(id, dimension, state string) error
	UpdateVersion(ID string, version *models.Version) error
	UpsertContact(ID string, update interface{}) error
	UpsertDataset(ID string, datasetDoc *models.DatasetUpdate) error
	UpsertEdition(datasetID, edition string, editionDoc *models.EditionUpdate) error
	UpsertVersion(ID string, versionDoc *models.Version) error
	DeleteDataset(ID string) error
	DeleteEdition(ID string) error

	AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error
	SetInstanceIsPublished(ctx context.Context, instanceID string) error
	StreamCSVRows(ctx context.Context, filter *observation.Filter, limit *int) (observation.StreamRowReader, error)
}
