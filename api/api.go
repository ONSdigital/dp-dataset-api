package api

//go:generate moq -out ../mocks/mocks.go -pkg mocks . DownloadsGenerator

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	clientsidentity "github.com/ONSdigital/dp-api-clients-go/v2/identity"
	"github.com/ONSdigital/dp-dataset-api/application"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/dimension"
	"github.com/ONSdigital/dp-dataset-api/instance"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/pagination"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/url"
	filesAPISDK "github.com/ONSdigital/dp-files-api/sdk"
	dprequest "github.com/ONSdigital/dp-net/v3/request"
	"github.com/ONSdigital/dp-permissions-api/sdk"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"

	auth "github.com/ONSdigital/dp-authorisation/v2/authorisation"
)

const (
	//nolint:gosec // This is not a hardcoded credential.
	downloadServiceToken = "X-Download-Service-Token"
	updateVersionAction  = "updateVersion"
	hasDownloads         = "has_downloads"
)

var (
	trueStringified = strconv.FormatBool(true)

	datasetCreatePermission = "datasets:create"
	datasetReadPermission   = "datasets:read"
	datasetUpdatePermission = "datasets:update"
	datasetDeletePermission = "datasets:delete"

	datasetEditionVersionCreatePermission = "dataset-editions-versions:create"
	datasetEditionVersionReadPermission   = "dataset-editions-versions:read"
	datasetEditionVersionUpdatePermission = "dataset-editions-versions:update"
	datasetEditionVersionDeletePermission = "dataset-editions-versions:delete"

	datasetInstanceCreatePermission = "dataset-instances:create"
	datasetInstanceReadPermission   = "dataset-instances:read"
	datasetInstanceUpdatePermission = "dataset-instances:update"
)

// API provides an interface for the routes
type API interface {
	CreateDatasetAPI(string, *mux.Router, store.DataStore) *DatasetAPI
}

// DownloadsGenerator pre generates full file downloads for the specified dataset/edition/version
type DownloadsGenerator interface {
	Generate(ctx context.Context, datasetID, instanceID, edition, version string) error
}

// DatasetAPI manages importing filters against a dataset
type DatasetAPI struct {
	Router                    *mux.Router
	dataStore                 store.DataStore
	urlBuilder                *url.Builder
	enableURLRewriting        bool
	host                      string
	downloadServiceToken      string
	EnablePrePublishView      bool
	downloadGenerators        map[models.DatasetType]DownloadsGenerator
	enablePrivateEndpoints    bool
	enableDetachDataset       bool
	enableDeleteStaticVersion bool
	authMiddleware            auth.Middleware
	instancePublishedChecker  *instance.PublishCheck
	versionPublishedChecker   *PublishCheck
	MaxRequestOptions         int
	defaultLimit              int
	smDatasetAPI              *application.StateMachineDatasetAPI
	filesAPIClient            filesAPISDK.Clienter
	authToken                 string
	permissionsChecker        auth.PermissionsChecker
	idClient                  *clientsidentity.Client
}

// Setup creates a new Dataset API instance and register the API routes based on the application configuration.
func Setup(ctx context.Context, cfg *config.Configuration, router *mux.Router, dataStore store.DataStore, urlBuilder *url.Builder, downloadGenerators map[models.DatasetType]DownloadsGenerator, authMiddleware auth.Middleware, enableURLRewriting bool, smDatasetAPI *application.StateMachineDatasetAPI, permissionsChecker auth.PermissionsChecker, idClient *clientsidentity.Client) *DatasetAPI {
	api := &DatasetAPI{
		dataStore:                 dataStore,
		host:                      cfg.DatasetAPIURL,
		downloadServiceToken:      cfg.DownloadServiceSecretKey,
		EnablePrePublishView:      cfg.EnablePrivateEndpoints,
		Router:                    router,
		urlBuilder:                urlBuilder,
		enableURLRewriting:        enableURLRewriting,
		downloadGenerators:        downloadGenerators,
		enablePrivateEndpoints:    cfg.EnablePrivateEndpoints,
		enableDetachDataset:       cfg.EnableDetachDataset,
		enableDeleteStaticVersion: cfg.EnableDeleteStaticVersion,
		authMiddleware:            authMiddleware,
		versionPublishedChecker:   nil,
		instancePublishedChecker:  nil,
		MaxRequestOptions:         cfg.MaxRequestOptions,
		defaultLimit:              cfg.DefaultLimit,
		smDatasetAPI:              smDatasetAPI,
		permissionsChecker:        permissionsChecker,
		idClient:                  idClient,
	}

	paginator := pagination.NewPaginator(cfg.DefaultLimit, cfg.DefaultOffset, cfg.DefaultMaxLimit)

	if api.enablePrivateEndpoints {
		log.Info(ctx, "enabling private endpoints for dataset api")

		api.versionPublishedChecker = &PublishCheck{
			Datastore: api.dataStore.Backend,
		}

		api.instancePublishedChecker = &instance.PublishCheck{
			Datastore: api.dataStore.Backend,
		}

		instanceAPI := &instance.Store{
			Host:                api.host,
			Storer:              api.dataStore.Backend,
			EnableDetachDataset: api.enableDetachDataset,
			URLBuilder:          api.urlBuilder,
			EnableURLRewriting:  api.enableURLRewriting,
		}

		dimensionAPI := &dimension.Store{
			Host:               api.host,
			Storer:             api.dataStore.Backend,
			MaxRequestOptions:  api.MaxRequestOptions,
			URLBuilder:         api.urlBuilder,
			EnableURLRewriting: api.enableURLRewriting,
		}

		api.enablePrivateDatasetEndpoints(paginator)
		api.enablePrivateInstancesEndpoints(instanceAPI, paginator)
		api.enablePrivateDimensionsEndpoints(dimensionAPI, paginator)
	} else {
		log.Info(ctx, "enabling only public endpoints for dataset api")
		api.enablePublicEndpoints(paginator)
	}
	return api
}

// SetFilesAPIClient sets the files API client and auth token for the API
func (api *DatasetAPI) SetFilesAPIClient(client filesAPISDK.Clienter, authToken string) {
	api.filesAPIClient = client
	api.authToken = authToken
}

// enablePublicEndpoints register only the public GET endpoints.
func (api *DatasetAPI) enablePublicEndpoints(paginator *pagination.Paginator) {
	api.get("/datasets", paginator.Paginate(api.getDatasets))
	api.get("/datasets/{dataset_id}", api.getDataset)
	api.get("/datasets/{dataset_id}/editions", paginator.Paginate(api.getEditions))
	api.get("/datasets/{dataset_id}/editions/{edition}", api.getEdition)
	api.get("/datasets/{dataset_id}/editions/{edition}/versions", paginator.Paginate(api.getVersions))
	api.get("/datasets/{dataset_id}/editions/{edition}/versions/{version}", contextAndErrors(api.getVersion))
	api.get("/datasets/{dataset_id}/editions/{edition}/versions/{version}/metadata", api.getMetadata)
	api.get("/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions", paginator.Paginate(api.getDimensions))
	api.get("/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions/{dimension}/options", paginator.Paginate(api.getDimensionOptions))
}

func writeErrorResponse(w http.ResponseWriter, errorResponse *models.ErrorResponse) {
	var jsonResponse []byte
	var err error
	w.Header().Set("Content-Type", "application/json")
	// process custom headers
	if errorResponse.Headers != nil {
		for key := range errorResponse.Headers {
			w.Header().Set(key, errorResponse.Headers[key])
		}
	}
	w.WriteHeader(errorResponse.Status)

	if errorResponse.Status == http.StatusInternalServerError {
		var filteredErrors []models.Error
		for _, err := range errorResponse.Errors {
			if !internalServerErrWithMessage[err.Cause] {
				err = models.NewError(err, models.InternalError, models.InternalErrorDescription)
			}
			filteredErrors = append(filteredErrors, err)
		}
		errorResponse.Errors = filteredErrors
	}

	jsonResponse, err = json.Marshal(errorResponse)
	if err != nil {
		responseErr := models.NewError(err, models.JSONMarshalError, models.ErrorMarshalFailedDescription)
		http.Error(w, responseErr.Description, http.StatusInternalServerError)
		return
	}

	_, err = w.Write(jsonResponse)
	if err != nil {
		responseErr := models.NewError(err, models.WriteResponseError, models.WriteResponseFailedDescription)
		http.Error(w, responseErr.Description, http.StatusInternalServerError)
		return
	}
}

func writeSuccessResponse(w http.ResponseWriter, successResponse *models.SuccessResponse) {
	w.Header().Set("Content-Type", "application/json")
	// process custom headers
	if successResponse.Headers != nil {
		for key := range successResponse.Headers {
			w.Header().Set(key, successResponse.Headers[key])
		}
	}
	w.WriteHeader(successResponse.Status)

	_, err := w.Write(successResponse.Body)
	if err != nil {
		responseErr := models.NewError(err, models.WriteResponseError, models.WriteResponseFailedDescription)
		http.Error(w, responseErr.Description, http.StatusInternalServerError)
		return
	}
}

type baseHandler func(w http.ResponseWriter, r *http.Request) (*models.SuccessResponse, *models.ErrorResponse)

func contextAndErrors(h baseHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		response, err := h(w, req)
		if err != nil {
			writeErrorResponse(w, err)
			return
		}
		writeSuccessResponse(w, response)
	}
}

// enablePrivateDatasetEndpoints register the datasets endpoints with the appropriate authentication and authorisation
// checks required when running the dataset API in publishing (private) mode.
func (api *DatasetAPI) enablePrivateDatasetEndpoints(paginator *pagination.Paginator) {
	api.get(
		"/datasets",
		api.authMiddleware.Require(datasetReadPermission, paginator.Paginate(api.getDatasets)),
	)

	api.get(
		"/datasets/{dataset_id}",
		api.authMiddleware.Require(datasetReadPermission, api.getDataset),
	)

	api.get(
		"/datasets/{dataset_id}/editions",
		api.authMiddleware.Require(datasetEditionVersionReadPermission, paginator.Paginate(api.getEditions)),
	)

	api.get(
		"/datasets/{dataset_id}/editions/{edition}",
		api.authMiddleware.Require(datasetEditionVersionReadPermission, api.getEdition),
	)

	api.get(
		"/datasets/{dataset_id}/editions/{edition}/versions",
		api.authMiddleware.Require(datasetEditionVersionReadPermission, paginator.Paginate(api.getVersions)),
	)

	api.get(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}",
		api.authMiddleware.Require(datasetEditionVersionReadPermission, contextAndErrors(api.getVersion)),
	)

	api.get(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}/metadata",
		api.authMiddleware.Require(datasetEditionVersionReadPermission, api.getMetadata),
	)

	api.get(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions",
		api.authMiddleware.Require(datasetEditionVersionReadPermission, paginator.Paginate(api.getDimensions)),
	)

	api.get(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions/{dimension}/options",
		api.authMiddleware.Require(datasetEditionVersionReadPermission, paginator.Paginate(api.getDimensionOptions)),
	)

	api.get(
		"/dataset-editions",
		api.authMiddleware.Require(datasetEditionVersionReadPermission, paginator.Paginate(api.getDatasetEditions)),
	)

	api.post(
		"/datasets/{dataset_id}",
		api.authMiddleware.Require(datasetCreatePermission, api.addDataset),
	)

	api.post(
		"/datasets",
		api.authMiddleware.Require(datasetCreatePermission, api.addDatasetNew),
	)

	api.put(
		"/datasets/{dataset_id}",
		api.authMiddleware.Require(datasetUpdatePermission, api.putDataset),
	)

	api.delete(
		"/datasets/{dataset_id}",
		api.authMiddleware.Require(datasetDeletePermission, api.deleteDataset),
	)

	api.put(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}",
		api.authMiddleware.Require(datasetEditionVersionUpdatePermission, api.isVersionPublished(updateVersionAction, api.putVersion)),
	)

	api.put(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}/metadata",
		api.authMiddleware.Require(datasetEditionVersionUpdatePermission, api.putMetadata),
	)

	api.put(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}/state",
		api.authMiddleware.Require(datasetEditionVersionUpdatePermission, api.putState),
	)

	api.post(
		"/datasets/{dataset_id}/editions/{edition}/versions",
		api.authMiddleware.Require(datasetEditionVersionCreatePermission, contextAndErrors(api.addDatasetVersionCondensed)),
	)

	api.post(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}",
		api.authMiddleware.Require(datasetEditionVersionCreatePermission, contextAndErrors(api.createVersion)),
	)

	api.delete(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}",
		api.authMiddleware.Require(datasetEditionVersionDeletePermission, api.deleteVersion),
	)
}

// enablePrivateInstancesEndpoints register the instance endpoints with the appropriate authentication and authorisation
// checks required when running the dataset API in publishing (private) mode.
func (api *DatasetAPI) enablePrivateInstancesEndpoints(instanceAPI *instance.Store, paginator *pagination.Paginator) {
	api.get(
		"/instances",
		api.authMiddleware.Require(datasetInstanceReadPermission, paginator.Paginate(instanceAPI.GetList)),
	)

	api.post(
		"/instances",
		api.authMiddleware.Require(datasetInstanceCreatePermission, instanceAPI.Add),
	)

	api.get(
		"/instances/{instance_id}",
		api.authMiddleware.Require(datasetInstanceReadPermission, instanceAPI.Get),
	)

	api.put(
		"/instances/{instance_id}",
		api.authMiddleware.Require(datasetInstanceUpdatePermission, api.isInstancePublished(instanceAPI.Update)),
	)

	api.put(
		"/instances/{instance_id}/dimensions/{dimension}",
		api.authMiddleware.Require(datasetInstanceUpdatePermission, api.isInstancePublished(instanceAPI.UpdateDimension)),
	)

	api.post(
		"/instances/{instance_id}/events",
		api.authMiddleware.Require(datasetInstanceCreatePermission, instanceAPI.AddEvent),
	)

	api.put(
		"/instances/{instance_id}/inserted_observations/{inserted_observations}",
		api.authMiddleware.Require(datasetInstanceUpdatePermission, api.isInstancePublished(instanceAPI.UpdateObservations)),
	)

	api.put(
		"/instances/{instance_id}/import_tasks",
		api.authMiddleware.Require(datasetInstanceUpdatePermission, api.isInstancePublished(instanceAPI.UpdateImportTask)),
	)
}

// enablePrivateDatasetEndpoints register the dimenions endpoints with the appropriate authentication and authorisation
// checks required when running the dataset API in publishing (private) mode.
func (api *DatasetAPI) enablePrivateDimensionsEndpoints(dimensionAPI *dimension.Store, paginator *pagination.Paginator) {
	api.get(
		"/instances/{instance_id}/dimensions",
		api.authMiddleware.Require(datasetInstanceReadPermission, paginator.Paginate(dimensionAPI.GetDimensionsHandler)),
	)

	// Deprecated (use patch /instances/{instance_id}/dimensions instead)
	api.post(
		"/instances/{instance_id}/dimensions",
		api.authMiddleware.Require(datasetInstanceCreatePermission, api.isInstancePublished(dimensionAPI.AddHandler)),
	)

	api.patch(
		"/instances/{instance_id}/dimensions",
		api.authMiddleware.Require(datasetInstanceUpdatePermission, api.isInstancePublished(dimensionAPI.PatchDimensionsHandler)),
	)

	api.get(
		"/instances/{instance_id}/dimensions/{dimension}/options",
		api.authMiddleware.Require(datasetInstanceReadPermission, paginator.Paginate(dimensionAPI.GetUniqueDimensionAndOptionsHandler)),
	)

	api.patch(
		"/instances/{instance_id}/dimensions/{dimension}/options/{option}",
		api.authMiddleware.Require(datasetInstanceUpdatePermission, api.isInstancePublished(dimensionAPI.PatchOptionHandler)),
	)

	// Deprecated (use patch /instances/{instance_id}/dimensions/{dimension}/options/{option} instead)
	//nolint:staticcheck // Accept deprecated AddNodeIDHandler for legacy support
	api.put(
		"/instances/{instance_id}/dimensions/{dimension}/options/{option}/node_id/{node_id}",
		api.authMiddleware.Require(datasetInstanceUpdatePermission, api.isInstancePublished(dimensionAPI.AddNodeIDHandler)),
	)
}

// isInstancePublished wraps a http.HandlerFunc checking the instance state. The wrapped handler is only invoked if the
// requested instance is not in a published state.
func (api *DatasetAPI) isInstancePublished(handler http.HandlerFunc) http.HandlerFunc {
	return api.instancePublishedChecker.Check(handler)
}

// isInstancePublished wraps a http.HandlerFunc checking the version state. The wrapped handler is only invoked if the
// requested version is not in a published state.
func (api *DatasetAPI) isVersionPublished(action string, handler http.HandlerFunc) http.HandlerFunc {
	return api.versionPublishedChecker.Check(handler, action)
}

// get registers a GET http.HandlerFunc.
func (api *DatasetAPI) get(path string, handler http.HandlerFunc) {
	api.Router.HandleFunc(path, handler).Methods(http.MethodGet)
}

// put registers a PUT http.HandlerFunc.
func (api *DatasetAPI) put(path string, handler http.HandlerFunc) {
	api.Router.HandleFunc(path, handler).Methods(http.MethodPut)
}

// patch registers a PATCH http.HandlerFunc
func (api *DatasetAPI) patch(path string, handler http.HandlerFunc) {
	api.Router.HandleFunc(path, handler).Methods(http.MethodPatch)
}

// post registers a POST http.HandlerFunc.
func (api *DatasetAPI) post(path string, handler http.HandlerFunc) {
	api.Router.HandleFunc(path, handler).Methods(http.MethodPost)
}

// delete registers a DELETE http.HandlerFunc.
func (api *DatasetAPI) delete(path string, handler http.HandlerFunc) {
	api.Router.HandleFunc(path, handler).Methods(http.MethodDelete)
}

// checks the user permission within a function to determine access to pre-publish data
func (api *DatasetAPI) checkUserPermission(r *http.Request, logData log.Data, permission string) bool {
	var authorised bool

	if api.EnablePrePublishView {
		bearerToken := strings.TrimPrefix(r.Header.Get(dprequest.AuthHeaderKey), dprequest.BearerPrefix)

		entityData, err := api.authMiddleware.Parse(bearerToken)
		if err != nil {
			// check service id token is valid
			resp, err := api.idClient.CheckTokenIdentity(r.Context(), bearerToken, clientsidentity.TokenTypeService)
			if err != nil {
				return false
			}
			// valid
			entityData = &sdk.EntityData{UserID: resp.Identifier}
		}
		logData["entity_data"] = entityData

		hasPermission, err := api.permissionsChecker.HasPermission(r.Context(), *entityData, permission, nil)
		if err != nil {
			return false
		}

		if hasPermission {
			authorised = true
		}

		return authorised
	}
	return authorised
}

func setJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}
