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
	UpsertDimensionsToInstance(dimensions []*models.CachedDimensionOption) error
	AddEventToInstance(currentInstance *models.Instance, event *models.Event, eTagSelector string) (newETag string, err error)
	AddInstance(instance *models.Instance) (*models.Instance, error)
	CheckDatasetExists(ID, state string) error
	CheckEditionExists(ID, editionID, state string) error
	GetDataset(ID string) (*models.DatasetUpdate, error)
	GetDatasets(ctx context.Context, offset, limit int, authorised bool) ([]*models.DatasetUpdate, int, error)
	GetDimensionsFromInstance(ctx context.Context, ID string, offset, limit int) ([]*models.DimensionOption, int, error)
	GetDimensions(datasetID, versionID string) ([]bson.M, error)
	GetDimensionOptions(ctx context.Context, version *models.Version, dimension string, offset, limit int) ([]*models.PublicDimensionOption, int, error)
	GetDimensionOptionsFromIDs(version *models.Version, dimension string, ids []string) ([]*models.PublicDimensionOption, int, error)
	GetEdition(ID, editionID, state string) (*models.EditionUpdate, error)
	GetEditions(ctx context.Context, ID, state string, offset, limit int, authorised bool) ([]*models.EditionUpdate, int, error)
	GetInstances(ctx context.Context, states []string, datasets []string, offset, limit int) ([]*models.Instance, int, error)
	GetInstance(ID, eTagSelector string) (*models.Instance, error)
	GetNextVersion(datasetID, editionID string) (int, error)
	GetVersion(datasetID, editionID string, version int, state string) (*models.Version, error)
	GetUniqueDimensionAndOptions(ctx context.Context, ID, dimension string, offset, limit int) ([]*string, int, error)
	GetVersions(ctx context.Context, datasetID, editionID, state string, offset, limit int) ([]models.Version, int, error)
	UpdateDataset(ctx context.Context, ID string, dataset *models.Dataset, currentState string) error
	UpdateDatasetWithAssociation(ID, state string, version *models.Version) error
	UpdateDimensionsNodeIDAndOrder(updates []*models.DimensionOption) error
	UpdateInstance(ctx context.Context, currentInstance, updatedInstance *models.Instance, eTagSelector string) (newETag string, err error)
	UpdateObservationInserted(currentInstance *models.Instance, observationInserted int64, eTagSelector string) (newETag string, err error)
	UpdateImportObservationsTaskState(currentInstance *models.Instance, state, eTagSelector string) (newETag string, err error)
	UpdateBuildHierarchyTaskState(currentInstance *models.Instance, dimension, state, eTagSelector string) (newETag string, err error)
	UpdateBuildSearchTaskState(currentInstance *models.Instance, dimension, state, eTagSelector string) (newETag string, err error)
	UpdateETagForOptions(currentInstance *models.Instance, upserts []*models.CachedDimensionOption, updates []*models.DimensionOption, eTagSelector string) (newETag string, err error)
	UpdateVersion(ID string, version *models.Version) error
	UpsertContact(ID string, update interface{}) error
	UpsertDataset(ID string, datasetDoc *models.DatasetUpdate) error
	UpsertEdition(datasetID, edition string, editionDoc *models.EditionUpdate) error
	UpsertVersion(ID string, versionDoc *models.Version) error
	DeleteDataset(ID string) error
	DeleteEdition(ID string) error
	AcquireInstanceLock(ctx context.Context, instanceID string) (lockID string, err error)
	UnlockInstance(lockID string)
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
