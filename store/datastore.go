package store

import (
	"context"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"github.com/globalsign/mgo/bson"
)

// DataStore provides a datastore.Storer interface used to store, retrieve, remove or update datasets
type DataStore struct {
	Backend Storer
}

//go:generate moq -out datastoretest/mongo.go -pkg storetest . MongoDB
//go:generate moq -out datastoretest/graph.go -pkg storetest . GraphDB
//go:generate moq -out datastoretest/datastore.go -pkg storetest . Storer

// dataMongoDB represents the required methos to access data from mongoDB
type dataMongoDB interface {
	AddDimensionToInstance(dimension *models.CachedDimensionOption) error
	AddEventToInstance(instanceID string, event *models.Event) error
	AddInstance(instance *models.Instance) (*models.Instance, error)
	CheckDatasetExists(ID, state string) error
	CheckEditionExists(ID, editionID, state string) error
	GetDataset(ID string) (*models.DatasetUpdate, error)
	GetDatasets(ctx context.Context, offset, limit int, authorised bool) ([]*models.DatasetUpdate, int, error)
	GetDimensionsFromInstance(ID string) (*models.DimensionNodeResults, error)
	GetDimensions(datasetID, versionID string) ([]bson.M, error)
	GetDimensionOptions(version *models.Version, dimension string, offset, limit int) (*models.DimensionOptionResults, error)
	GetDimensionOptionsFromIDs(version *models.Version, dimension string, ids []string) (*models.DimensionOptionResults, error)
	GetEdition(ID, editionID, state string) (*models.EditionUpdate, error)
	GetEditions(ctx context.Context, ID, state string, offset, limit int, authorised bool) ([]*models.EditionUpdate, int, error)
	GetInstances(ctx context.Context, states []string, datasets []string, offset, limit int) (*models.InstanceResults, error)
	GetInstance(ID string) (*models.Instance, error)
	GetNextVersion(datasetID, editionID string) (int, error)
	GetUniqueDimensionAndOptions(ID, dimension string) (*models.DimensionValues, error)
	GetVersion(datasetID, editionID, version, state string) (*models.Version, error)
	GetVersions(ctx context.Context, datasetID, editionID, state string, offset, limit int) (*models.VersionResults, error)
	UpdateDataset(ctx context.Context, ID string, dataset *models.Dataset, currentState string) error
	UpdateDatasetWithAssociation(ID, state string, version *models.Version) error
	UpdateDimensionNodeIDAndOrder(dimension *models.DimensionOption) error
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
}

// MongoDB represents all the required methods from mongo DB
type MongoDB interface {
	dataMongoDB
	Close(context.Context) error
	Checker(context.Context, *healthcheck.CheckState) error
}

// dataGraphDB represents the required methods to access data from GraphDB
type dataGraphDB interface {
	AddVersionDetailsToInstance(ctx context.Context, instanceID string, datasetID string, edition string, version int) error
	SetInstanceIsPublished(ctx context.Context, instanceID string) error
}

// GraphDB represents all the required methods from graph DB
type GraphDB interface {
	dataGraphDB
	Close(ctx context.Context) error
	Checker(context.Context, *healthcheck.CheckState) error
}

// Storer represents basic data access via Get, Remove and Upsert methods, abstracting it from mongoDB or graphDB
type Storer interface {
	dataMongoDB
	dataGraphDB
}
