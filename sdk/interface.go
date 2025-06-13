package sdk

import (
	"context"
	"net/http"
	"net/url"

	"github.com/ONSdigital/dp-api-clients-go/v2/health"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
)

//go:generate moq -out ./mocks/client.go -pkg mocks . Clienter

type Clienter interface {
	Checker(ctx context.Context, check *healthcheck.CheckState) error
	Health() *health.Client
	URL() string
	DoAuthenticatedGetRequest(ctx context.Context, headers Headers, uri *url.URL) (resp *http.Response, err error)
	GetDataset(ctx context.Context, headers Headers, collectionID, datasetID string) (dataset models.Dataset, err error)
	GetDatasetByPath(ctx context.Context, headers Headers, path string) (dataset models.Dataset, err error)
	GetEdition(ctx context.Context, headers Headers, datasetID, editionID string) (edition models.Edition, err error)
	GetEditions(ctx context.Context, headers Headers, datasetID string, queryParams *QueryParams) (editionList EditionsList, err error)
	GetVersion(ctx context.Context, headers Headers, datasetID, editionID, versionID string) (version models.Version, err error)
	GetVersionDimensions(ctx context.Context, headers Headers, datasetID, editionID, versionID string) (versionDimensionsList VersionDimensionsList, err error)
	GetVersionDimensionOptions(ctx context.Context, headers Headers, datasetID, editionID, versionID, dimensionID string, queryParams *QueryParams) (versionDimensionOptionsList VersionDimensionOptionsList, err error)
	GetVersionMetadata(ctx context.Context, headers Headers, datasetID, editionID, versionID string) (metadata models.Metadata, err error)
	GetVersions(ctx context.Context, headers Headers, datasetID, editionID string, queryParams *QueryParams) (versionsList VersionsList, err error)
	PutVersionState(ctx context.Context, headers Headers, datasetID, editionID, versionID, state string) (err error)
}
