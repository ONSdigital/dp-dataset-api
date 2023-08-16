package store

import (
	"context"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"go.mongodb.org/mongo-driver/bson"
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
	UpsertDimensionsToInstance(ctx context.Context, dimensions []*models.CachedDimensionOption) error
	AddEventToInstance(ctx context.Context, currentInstance *models.Instance, event *models.Event, eTagSelector string) (newETag string, err error)
	AddInstance(ctx context.Context, instance *models.Instance) (*models.Instance, error)
	CheckDatasetExists(ctx context.Context, ID, state string) error
	CheckEditionExists(ctx context.Context, ID, editionID, state string) error
	GetDataset(ctx context.Context, ID string) (*models.DatasetUpdate, error)
	GetDatasets(ctx context.Context, offset, limit int, authorised bool) ([]*models.DatasetUpdate, int, error)
	GetDatasetsByBasedOn(ctx context.Context, ID string, offset, limit int, authorised bool) ([]*models.DatasetUpdate, int, error)
	GetDimensionsFromInstance(ctx context.Context, ID string, offset, limit int) ([]*models.DimensionOption, int, error)
	GetDimensions(ctx context.Context, versionID string) ([]bson.M, error)
	GetDimensionOptions(ctx context.Context, version *models.Version, dimension string, offset, limit int) ([]*models.PublicDimensionOption, int, error)
	GetDimensionOptionsFromIDs(ctx context.Context, version *models.Version, dimension string, ids []string) ([]*models.PublicDimensionOption, int, error)
	GetEdition(ctx context.Context, ID, editionID, state string) (*models.EditionUpdate, error)
	GetEditions(ctx context.Context, ID, state string, offset, limit int, authorised bool) ([]*models.EditionUpdate, int, error)
	GetInstances(ctx context.Context, states []string, datasets []string, offset, limit int) ([]*models.Instance, int, error)
	GetInstance(ctx context.Context, ID, eTagSelector string) (*models.Instance, error)
	GetNextVersion(ctx context.Context, datasetID, editionID string) (int, error)
	GetVersion(ctx context.Context, datasetID, editionID string, version int, state string) (*models.Version, error)
	GetUniqueDimensionAndOptions(ctx context.Context, ID, dimension string) ([]*string, int, error)
	GetVersions(ctx context.Context, datasetID, editionID, state string, offset, limit int) ([]models.Version, int, error)
	UpdateDataset(ctx context.Context, ID string, dataset *models.Dataset, currentState string) error
	UpdateDatasetWithAssociation(ctx context.Context, ID, state string, version *models.Version) error
	UpdateDimensionsNodeIDAndOrder(ctx context.Context, updates []*models.DimensionOption) error
	UpdateInstance(ctx context.Context, currentInstance, updatedInstance *models.Instance, eTagSelector string) (newETag string, err error)
	UpdateObservationInserted(ctx context.Context, currentInstance *models.Instance, observationInserted int64, eTagSelector string) (newETag string, err error)
	UpdateImportObservationsTaskState(ctx context.Context, currentInstance *models.Instance, state, eTagSelector string) (newETag string, err error)
	UpdateBuildHierarchyTaskState(ctx context.Context, currentInstance *models.Instance, dimension, state, eTagSelector string) (newETag string, err error)
	UpdateBuildSearchTaskState(ctx context.Context, currentInstance *models.Instance, dimension, state, eTagSelector string) (newETag string, err error)
	UpdateETagForOptions(ctx context.Context, currentInstance *models.Instance, upserts []*models.CachedDimensionOption, updates []*models.DimensionOption, eTagSelector string) (newETag string, err error)
	UpdateVersion(ctx context.Context, currentVersion *models.Version, version *models.Version, eTagSelector string) (newETag string, err error)
	UpdateMetadata(ctx context.Context, datasetID string, versionID string, versionEtag string, updatedDataset *models.Dataset, updatedVersion *models.Version) error
	UpsertContact(ctx context.Context, ID string, update interface{}) error
	UpsertDataset(ctx context.Context, ID string, datasetDoc *models.DatasetUpdate) error
	UpsertEdition(ctx context.Context, datasetID, edition string, editionDoc *models.EditionUpdate) error
	UpsertVersion(ctx context.Context, ID string, versionDoc *models.Version) error
	DeleteDataset(ctx context.Context, ID string) error
	DeleteEdition(ctx context.Context, ID string) error
	AcquireInstanceLock(ctx context.Context, instanceID string) (lockID string, err error)
	UnlockInstance(ctx context.Context, lockID string)
	RemoveDatasetVersionAndEditionLinks(ctx context.Context, id string) error
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
