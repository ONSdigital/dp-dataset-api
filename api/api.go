package api

//go:generate moq -out ../mocks/mocks.go -pkg mocks . DownloadsGenerator

import (
	"context"
	"net/http"
	"strconv"

	"github.com/ONSdigital/dp-authorisation/auth"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/dimension"
	"github.com/ONSdigital/dp-dataset-api/instance"
	"github.com/ONSdigital/dp-dataset-api/pagination"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/url"
	dphandlers "github.com/ONSdigital/dp-net/handlers"
	dprequest "github.com/ONSdigital/dp-net/request"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

const (
	downloadServiceToken = "X-Download-Service-Token"
	updateVersionAction  = "updateVersion"
	hasDownloads         = "has_downloads"
)

var (
	trueStringified = strconv.FormatBool(true)

	createPermission = auth.Permissions{Create: true}
	readPermission   = auth.Permissions{Read: true}
	updatePermission = auth.Permissions{Update: true}
	deletePermission = auth.Permissions{Delete: true}
)

//API provides an interface for the routes
type API interface {
	CreateDatasetAPI(string, *mux.Router, store.DataStore) *DatasetAPI
}

// DownloadsGenerator pre generates full file downloads for the specified dataset/edition/version
type DownloadsGenerator interface {
	Generate(ctx context.Context, datasetID, instanceID, edition, version string) error
}

// AuthHandler provides authorisation checks on requests
type AuthHandler interface {
	Require(required auth.Permissions, handler http.HandlerFunc) http.HandlerFunc
}

// DatasetAPI manages importing filters against a dataset
type DatasetAPI struct {
	Router                   *mux.Router
	dataStore                store.DataStore
	urlBuilder               *url.Builder
	host                     string
	downloadServiceToken     string
	EnablePrePublishView     bool
	downloadGenerator        DownloadsGenerator
	enablePrivateEndpoints   bool
	enableDetachDataset      bool
	datasetPermissions       AuthHandler
	permissions              AuthHandler
	instancePublishedChecker *instance.PublishCheck
	versionPublishedChecker  *PublishCheck
	defaultLimit             int
	defaultOffset            int
	maxLimit                 int
}

// Setup creates a new Dataset API instance and register the API routes based on the application configuration.
func Setup(ctx context.Context, cfg *config.Configuration, router *mux.Router, dataStore store.DataStore, urlBuilder *url.Builder, downloadGenerator DownloadsGenerator, datasetPermissions AuthHandler, permissions AuthHandler) *DatasetAPI {

	api := &DatasetAPI{
		dataStore:                dataStore,
		host:                     cfg.DatasetAPIURL,
		downloadServiceToken:     cfg.DownloadServiceSecretKey,
		EnablePrePublishView:     cfg.EnablePrivateEndpoints,
		Router:                   router,
		urlBuilder:               urlBuilder,
		downloadGenerator:        downloadGenerator,
		enablePrivateEndpoints:   cfg.EnablePrivateEndpoints,
		enableDetachDataset:      cfg.EnableDetachDataset,
		datasetPermissions:       datasetPermissions,
		permissions:              permissions,
		versionPublishedChecker:  nil,
		instancePublishedChecker: nil,
		defaultLimit:             cfg.DefaultLimit,
		defaultOffset:            cfg.DefaultOffset,
		maxLimit:                 cfg.DefaultMaxLimit,
	}

	paginator := pagination.NewPaginator(cfg.DefaultLimit, cfg.DefaultOffset, cfg.DefaultMaxLimit)

	if api.enablePrivateEndpoints {
		log.Event(ctx, "enabling private endpoints for dataset api", log.INFO)

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
			DefaultOffset:       api.defaultOffset,
			DefaultLimit:        api.defaultLimit,
		}

		dimensionAPI := &dimension.Store{
			Storer: api.dataStore.Backend,
		}

		api.enablePrivateDatasetEndpoints(ctx, paginator)
		api.enablePrivateInstancesEndpoints(instanceAPI)
		api.enablePrivateDimensionsEndpoints(dimensionAPI)
	} else {
		log.Event(ctx, "enabling only public endpoints for dataset api", log.INFO)
		api.enablePublicEndpoints(ctx, paginator)
	}
	return api
}

// enablePublicEndpoints register only the public GET endpoints.
func (api *DatasetAPI) enablePublicEndpoints(ctx context.Context, paginator *pagination.Paginator) {
	api.get("/datasets", paginator.Paginate(api.getDatasets))
	api.get("/datasets/{dataset_id}", api.getDataset)
	api.get("/datasets/{dataset_id}/editions", paginator.Paginate(api.getEditions))
	api.get("/datasets/{dataset_id}/editions/{edition}", api.getEdition)
	api.get("/datasets/{dataset_id}/editions/{edition}/versions", paginator.Paginate(api.getVersions))
	api.get("/datasets/{dataset_id}/editions/{edition}/versions/{version}", api.getVersion)
	api.get("/datasets/{dataset_id}/editions/{edition}/versions/{version}/metadata", api.getMetadata)
	api.get("/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions", api.getDimensions)
	api.get("/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions/{dimension}/options", api.getDimensionOptions)

}

// enablePrivateDatasetEndpoints register the datasets endpoints with the appropriate authentication and authorisation
// checks required when running the dataset API in publishing (private) mode.
func (api *DatasetAPI) enablePrivateDatasetEndpoints(ctx context.Context, paginator *pagination.Paginator) {
	api.get(
		"/datasets",
		api.isAuthorised(readPermission, paginator.Paginate(api.getDatasets)),
	)

	api.get(
		"/datasets/{dataset_id}",
		api.isAuthorisedForDatasets(readPermission,
			api.getDataset),
	)

	api.get(
		"/datasets/{dataset_id}/editions",
		api.isAuthorisedForDatasets(readPermission, paginator.Paginate(api.getEditions)),
	)

	api.get(
		"/datasets/{dataset_id}/editions/{edition}",
		api.isAuthorisedForDatasets(readPermission,
			api.getEdition),
	)

	api.get(
		"/datasets/{dataset_id}/editions/{edition}/versions",
		api.isAuthorisedForDatasets(readPermission,
			paginator.Paginate(api.getVersions)),
	)

	api.get(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}",
		api.isAuthorisedForDatasets(readPermission,
			api.getVersion),
	)

	api.get(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}/metadata",
		api.isAuthorisedForDatasets(readPermission,
			api.getMetadata),
	)

	api.get(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions",
		api.isAuthorisedForDatasets(readPermission,
			api.getDimensions),
	)

	api.get(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions/{dimension}/options",
		api.isAuthorisedForDatasets(readPermission,
			api.getDimensionOptions),
	)

	api.post(
		"/datasets/{dataset_id}",
		api.isAuthenticated(
			api.isAuthorisedForDatasets(createPermission,
				api.addDataset)),
	)

	api.put(
		"/datasets/{dataset_id}",
		api.isAuthenticated(
			api.isAuthorisedForDatasets(updatePermission,
				api.putDataset)),
	)

	api.delete(
		"/datasets/{dataset_id}",
		api.isAuthenticated(
			api.isAuthorisedForDatasets(deletePermission,
				api.deleteDataset)),
	)

	api.put(
		"/datasets/{dataset_id}/editions/{edition}/versions/{version}",
		api.isAuthenticated(
			api.isAuthorisedForDatasets(updatePermission,
				api.isVersionPublished(updateVersionAction,
					api.putVersion))),
	)

	if api.enableDetachDataset {
		api.delete(
			"/datasets/{dataset_id}/editions/{edition}/versions/{version}",
			api.isAuthenticated(
				api.isAuthorisedForDatasets(deletePermission,
					api.detachVersion)),
		)
	}
}

// enablePrivateInstancesEndpoints register the instance endpoints with the appropriate authentication and authorisation
// checks required when running the dataset API in publishing (private) mode.
func (api *DatasetAPI) enablePrivateInstancesEndpoints(instanceAPI *instance.Store) {
	api.get(
		"/instances",
		api.isAuthenticated(
			api.isAuthorised(readPermission,
				instanceAPI.GetList)),
	)

	api.post(
		"/instances",
		api.isAuthenticated(
			api.isAuthorised(createPermission,
				instanceAPI.Add)),
	)

	api.get(
		"/instances/{instance_id}",
		api.isAuthenticated(
			api.isAuthorised(readPermission,
				instanceAPI.Get)),
	)

	api.put(
		"/instances/{instance_id}",
		api.isAuthenticated(
			api.isAuthorised(updatePermission,
				api.isInstancePublished(instanceAPI.Update))),
	)

	api.put(
		"/instances/{instance_id}/dimensions/{dimension}",
		api.isAuthenticated(
			api.isAuthorised(updatePermission,
				api.isInstancePublished(instanceAPI.UpdateDimension))),
	)

	api.post(
		"/instances/{instance_id}/events",
		api.isAuthenticated(
			api.isAuthorised(createPermission,
				instanceAPI.AddEvent)),
	)

	api.put(
		"/instances/{instance_id}/inserted_observations/{inserted_observations}",
		api.isAuthenticated(
			api.isAuthorised(updatePermission,
				api.isInstancePublished(instanceAPI.UpdateObservations))),
	)

	api.put(
		"/instances/{instance_id}/import_tasks",
		api.isAuthenticated(
			api.isAuthorised(updatePermission,
				api.isInstancePublished(instanceAPI.UpdateImportTask))),
	)
}

// enablePrivateDatasetEndpoints register the dimenions endpoints with the appropriate authentication and authorisation
// checks required when running the dataset API in publishing (private) mode.
func (api *DatasetAPI) enablePrivateDimensionsEndpoints(dimensionAPI *dimension.Store) {
	api.get(
		"/instances/{instance_id}/dimensions",
		api.isAuthenticated(
			api.isAuthorised(readPermission,
				dimensionAPI.GetDimensionsHandler)),
	)

	api.post(
		"/instances/{instance_id}/dimensions",
		api.isAuthenticated(
			api.isAuthorised(createPermission,
				api.isInstancePublished(dimensionAPI.AddHandler))),
	)

	api.get(
		"/instances/{instance_id}/dimensions/{dimension}/options",
		api.isAuthenticated(
			api.isAuthorised(readPermission,
				dimensionAPI.GetUniqueDimensionAndOptionsHandler)),
	)

	api.patch(
		"/instances/{instance_id}/dimensions/{dimension}/options/{option}",
		api.isAuthenticated(
			api.isAuthorised(updatePermission,
				api.isInstancePublished(dimensionAPI.PatchOptionHandler))),
	)

	// Deprecated
	api.put(
		"/instances/{instance_id}/dimensions/{dimension}/options/{option}/node_id/{node_id}",
		api.isAuthenticated(
			api.isAuthorised(updatePermission,
				api.isInstancePublished(dimensionAPI.AddNodeIDHandler))),
	)
}

// isAuthenticated wraps a http handler func in another http handler func that checks the caller is authenticated to
// perform the requested action. handler is the http.HandlerFunc to wrap in an
// authentication check. The wrapped handler is only called if the caller is authenticated
func (api *DatasetAPI) isAuthenticated(handler http.HandlerFunc) http.HandlerFunc {
	return dphandlers.CheckIdentity(handler)
}

// isAuthorised wraps a http.HandlerFunc another http.HandlerFunc that checks the caller is authorised to perform the
// requested action. required is the permissions required to perform the action, handler is the http.HandlerFunc to
// apply the check to. The wrapped handler is only called if the caller has the required permissions.
func (api *DatasetAPI) isAuthorised(required auth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
	return api.permissions.Require(required, handler)
}

// isAuthorised wraps a http.HandlerFunc another http.HandlerFunc that checks the caller is authorised to perform the
// requested datasets action. This authorisation check is specific to datastes. required is the permissions required to
// perform the action, handler is the http.HandlerFunc to apply the check to. The wrapped handler is only called if the
// caller has the required dataset permissions.
func (api *DatasetAPI) isAuthorisedForDatasets(required auth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
	return api.datasetPermissions.Require(required, handler)
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

func (api *DatasetAPI) authenticate(r *http.Request, logData log.Data) bool {
	var authorised bool

	if api.EnablePrePublishView {
		var hasCallerIdentity, hasUserIdentity bool

		callerIdentity := dprequest.Caller(r.Context())
		if callerIdentity != "" {
			logData["caller_identity"] = callerIdentity
			hasCallerIdentity = true
		}

		userIdentity := dprequest.User(r.Context())
		if userIdentity != "" {
			logData["user_identity"] = userIdentity
			hasUserIdentity = true
		}

		if hasCallerIdentity || hasUserIdentity {
			authorised = true
		}
		logData["authenticated"] = authorised

		return authorised
	}
	return authorised
}

func setJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}
