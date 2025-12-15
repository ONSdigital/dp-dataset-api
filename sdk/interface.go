package sdk

import (
	"context"
	"net/http"

	"github.com/ONSdigital/dp-api-clients-go/v2/health"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
)

//go:generate moq -out ./mocks/client.go -pkg mocks . Clienter

type Clienter interface {
	Checker(ctx context.Context, check *healthcheck.CheckState) error
	Health() *health.Client
	URL() string

	CreateDataset(ctx context.Context, headers Headers, dataset models.Dataset) (datasetUpdate models.DatasetUpdate, err error)
	GetDataset(ctx context.Context, headers Headers, datasetID string) (dataset models.Dataset, err error)
	GetDatasetByPath(ctx context.Context, headers Headers, path string) (dataset models.Dataset, err error)
	GetDatasetCurrentAndNext(ctx context.Context, headers Headers, datasetID string) (dataset models.DatasetUpdate, err error)
	GetDatasetEditions(ctx context.Context, headers Headers, queryParams *QueryParams) (datasetEditionsList DatasetEditionsList, err error)
	GetDatasets(ctx context.Context, headers Headers, q *QueryParams) (datasets DatasetsList, err error)
	GetDatasetsInBatches(ctx context.Context, headers Headers, batchSize, maxWorkers int) (datasets DatasetsList, err error)
	GetEdition(ctx context.Context, headers Headers, datasetID, editionID string) (edition models.Edition, err error)
	GetEditions(ctx context.Context, headers Headers, datasetID string, queryParams *QueryParams) (editionList EditionsList, err error)
	GetVersion(ctx context.Context, headers Headers, datasetID, editionID, versionID string) (version models.Version, err error)
	GetVersionDimensions(ctx context.Context, headers Headers, datasetID, editionID, versionID string) (versionDimensionsList VersionDimensionsList, err error)
	GetVersionDimensionOptions(ctx context.Context, headers Headers, datasetID, editionID, versionID, dimensionID string, queryParams *QueryParams) (versionDimensionOptionsList VersionDimensionOptionsList, err error)
	GetVersionMetadata(ctx context.Context, headers Headers, datasetID, editionID, versionID string) (metadata models.Metadata, err error)
	GetVersionWithHeaders(ctx context.Context, headers Headers, datasetID, edition, version string) (v models.Version, h ResponseHeaders, err error)
	GetVersionWithResponse(ctx context.Context, headers Headers, datasetID, edition, versionID string) (v models.Version, resp *http.Response, err error)
	GetVersions(ctx context.Context, headers Headers, datasetID, editionID string, queryParams *QueryParams) (versionsList VersionsList, err error)
	GetVersionsInBatches(ctx context.Context, headers Headers, datasetID, edition string, batchSize, maxWorkers int) (versions VersionsList, err error)
	PostVersion(ctx context.Context, headers Headers, datasetID, editionID, versionID string, version models.Version) (createdVersion *models.Version, err error)
	PutDataset(ctx context.Context, headers Headers, datasetID string, d models.Dataset) error
	PutInstance(ctx context.Context, headers Headers, instanceID string, i UpdateInstance, ifMatch string) (eTag string, err error)
	PutMetadata(ctx context.Context, headers Headers, datasetID, edition, version string, metadata models.EditableMetadata, versionEtag string) error
	PutVersion(ctx context.Context, headers Headers, datasetID, editionID, versionID string, version models.Version) (updatedVersion models.Version, err error)
	PutVersionState(ctx context.Context, headers Headers, datasetID, editionID, versionID, state string) (err error)
}
