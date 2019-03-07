package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/dimension"
	"github.com/ONSdigital/dp-dataset-api/instance"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/healthcheck"
	"github.com/ONSdigital/go-ns/identity"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/request"
	"github.com/ONSdigital/go-ns/server"
	"github.com/gorilla/mux"
	"github.com/justinas/alice"
	"github.com/pkg/errors"
)

var httpServer *server.Server

const (
	downloadServiceToken   = "X-Download-Service-Token"
	dimensionDocType       = "dimension"
	dimensionOptionDocType = "dimension-option"

	// audit actions
	addDatasetAction    = "addDataset"
	deleteDatasetAction = "deleteDataset"
	getDatasetsAction   = "getDatasets"
	getDatasetAction    = "getDataset"

	getEditionsAction = "getEditions"
	getEditionAction  = "getEdition"

	getVersionsAction      = "getVersions"
	getVersionAction       = "getVersion"
	updateDatasetAction    = "updateDataset"
	updateVersionAction    = "updateVersion"
	associateVersionAction = "associateVersionAction"
	publishVersionAction   = "publishVersion"

	getDimensionsAction       = "getDimensions"
	getDimensionOptionsAction = "getDimensionOptionsAction"
	getMetadataAction         = "getMetadata"

	auditError     = "error while attempting to record audit event, failing request"
	auditActionErr = "failed to audit action"

	hasDownloads = "has_downloads"
)

var trueStringified = strconv.FormatBool(true)

// PublishCheck Checks if an version has been published
type PublishCheck struct {
	Datastore store.Storer
	Auditor   audit.AuditorService
}

//API provides an interface for the routes
type API interface {
	CreateDatasetAPI(string, *mux.Router, store.DataStore) *DatasetAPI
}

// DownloadsGenerator pre generates full file downloads for the specified dataset/edition/version
type DownloadsGenerator interface {
	Generate(datasetID, instanceID, edition, version string) error
}

// Auditor is an alias for the auditor service
type Auditor audit.AuditorService

// DatasetAPI manages importing filters against a dataset
type DatasetAPI struct {
	dataStore            store.DataStore
	host                 string
	zebedeeURL           string
	internalToken        string
	downloadServiceToken string
	EnablePrePublishView bool
	Router               *mux.Router
	urlBuilder           *url.Builder
	downloadGenerator    DownloadsGenerator
	healthCheckTimeout   time.Duration
	serviceAuthToken     string
	auditor              Auditor
}

// CreateDatasetAPI manages all the routes configured to API
func CreateDatasetAPI(cfg config.Configuration, dataStore store.DataStore, urlBuilder *url.Builder, errorChan chan error, downloadsGenerator DownloadsGenerator, auditor Auditor) {
	router := mux.NewRouter()
	Routes(cfg, router, dataStore, urlBuilder, downloadsGenerator, auditor)

	healthcheckHandler := healthcheck.NewMiddleware(healthcheck.Do)
	middleware := alice.New(healthcheckHandler)

	// Only add the identity middleware when running in publishing.
	if cfg.EnablePrivateEnpoints {
		middleware = middleware.Append(identity.Handler(cfg.ZebedeeURL))
	}

	httpServer = server.New(cfg.BindAddr, middleware.Then(router))

	// Disable this here to allow main to manage graceful shutdown of the entire app.
	httpServer.HandleOSSignals = false

	go func() {
		log.Debug("Starting api...", nil)
		if err := httpServer.ListenAndServe(); err != nil {
			log.ErrorC("api http server returned error", err, nil)
			errorChan <- err
		}
	}()
}

// Routes represents a list of endpoints that exist with this api
func Routes(cfg config.Configuration, router *mux.Router, dataStore store.DataStore, urlBuilder *url.Builder, downloadGenerator DownloadsGenerator, auditor Auditor) *DatasetAPI {

	api := DatasetAPI{
		dataStore:            dataStore,
		host:                 cfg.DatasetAPIURL,
		zebedeeURL:           cfg.ZebedeeURL,
		serviceAuthToken:     cfg.ServiceAuthToken,
		downloadServiceToken: cfg.DownloadServiceSecretKey,
		EnablePrePublishView: cfg.EnablePrivateEnpoints,
		Router:               router,
		urlBuilder:           urlBuilder,
		downloadGenerator:    downloadGenerator,
		healthCheckTimeout:   cfg.HealthCheckTimeout,
		auditor:              auditor,
	}

	api.Router.HandleFunc("/datasets", api.getDatasets).Methods("GET")
	api.Router.HandleFunc("/datasets/{dataset_id}", api.getDataset).Methods("GET")
	api.Router.HandleFunc("/datasets/{dataset_id}/editions", api.getEditions).Methods("GET")
	api.Router.HandleFunc("/datasets/{dataset_id}/editions/{edition}", api.getEdition).Methods("GET")
	api.Router.HandleFunc("/datasets/{dataset_id}/editions/{edition}/versions", api.getVersions).Methods("GET")
	api.Router.HandleFunc("/datasets/{dataset_id}/editions/{edition}/versions/{version}", api.getVersion).Methods("GET")
	api.Router.HandleFunc("/datasets/{dataset_id}/editions/{edition}/versions/{version}/metadata", api.getMetadata).Methods("GET")
	api.Router.HandleFunc("/datasets/{dataset_id}/editions/{edition}/versions/{version}/observations", api.getObservations).Methods("GET")
	api.Router.HandleFunc("/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions", api.getDimensions).Methods("GET")
	api.Router.HandleFunc("/datasets/{dataset_id}/editions/{edition}/versions/{version}/dimensions/{dimension}/options", api.getDimensionOptions).Methods("GET")

	if cfg.EnablePrivateEnpoints {

		log.Debug("private endpoints have been enabled", nil)

		versionPublishChecker := PublishCheck{Auditor: auditor, Datastore: dataStore.Backend}
		api.Router.HandleFunc("/datasets/{dataset_id}", identity.Check(auditor, addDatasetAction, api.addDataset)).Methods("POST")
		api.Router.HandleFunc("/datasets/{dataset_id}", identity.Check(auditor, updateDatasetAction, api.putDataset)).Methods("PUT")
		api.Router.HandleFunc("/datasets/{dataset_id}", identity.Check(auditor, deleteDatasetAction, api.deleteDataset)).Methods("DELETE")
		api.Router.HandleFunc("/datasets/{dataset_id}/editions/{edition}/versions/{version}", identity.Check(auditor, updateVersionAction, versionPublishChecker.Check(api.putVersion, updateVersionAction))).Methods("PUT")

		instanceAPI := instance.Store{Host: api.host, Storer: api.dataStore.Backend, Auditor: auditor}
		instancePublishChecker := instance.PublishCheck{Auditor: auditor, Datastore: dataStore.Backend}
		api.Router.HandleFunc("/instances", identity.Check(auditor, instance.GetInstancesAction, instanceAPI.GetList)).Methods("GET")
		api.Router.HandleFunc("/instances", identity.Check(auditor, instance.AddInstanceAction, instanceAPI.Add)).Methods("POST")
		api.Router.HandleFunc("/instances/{instance_id}", identity.Check(auditor, instance.GetInstanceAction, instanceAPI.Get)).Methods("GET")
		api.Router.HandleFunc("/instances/{instance_id}", identity.Check(auditor, instance.UpdateInstanceAction, instancePublishChecker.Check(instanceAPI.Update, instance.UpdateInstanceAction))).Methods("PUT")
		api.Router.HandleFunc("/instances/{instance_id}/dimensions/{dimension}", identity.Check(auditor, instance.UpdateDimensionAction, instancePublishChecker.Check(instanceAPI.UpdateDimension, instance.UpdateDimensionAction))).Methods("PUT")
		api.Router.HandleFunc("/instances/{instance_id}/events", identity.Check(auditor, instance.AddInstanceEventAction, instanceAPI.AddEvent)).Methods("POST")
		api.Router.HandleFunc("/instances/{instance_id}/inserted_observations/{inserted_observations}",
			identity.Check(auditor, instance.UpdateInsertedObservationsAction, instancePublishChecker.Check(instanceAPI.UpdateObservations, instance.UpdateInsertedObservationsAction))).Methods("PUT")
		api.Router.HandleFunc("/instances/{instance_id}/import_tasks", identity.Check(auditor, instance.UpdateImportTasksAction, instancePublishChecker.Check(instanceAPI.UpdateImportTask, instance.UpdateImportTasksAction))).Methods("PUT")

		dimensionAPI := dimension.Store{Auditor: auditor, Storer: api.dataStore.Backend}
		api.Router.HandleFunc("/instances/{instance_id}/dimensions", identity.Check(auditor, dimension.GetDimensions, dimensionAPI.GetDimensionsHandler)).Methods("GET")
		api.Router.HandleFunc("/instances/{instance_id}/dimensions", identity.Check(auditor, dimension.AddDimensionAction, instancePublishChecker.Check(dimensionAPI.AddHandler, dimension.AddDimensionAction))).Methods("POST")
		api.Router.HandleFunc("/instances/{instance_id}/dimensions/{dimension}/options", identity.Check(auditor, dimension.GetUniqueDimensionAndOptionsAction, dimensionAPI.GetUniqueDimensionAndOptionsHandler)).Methods("GET")
		api.Router.HandleFunc("/instances/{instance_id}/dimensions/{dimension}/options/{option}/node_id/{node_id}",
			identity.Check(auditor, dimension.UpdateNodeIDAction, instancePublishChecker.Check(dimensionAPI.AddNodeIDHandler, dimension.UpdateNodeIDAction))).Methods("PUT")
	}
	return &api
}

// Check wraps a HTTP handle. Checks that the state is not published
func (d *PublishCheck) Check(handle func(http.ResponseWriter, *http.Request), action string) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		ctx := r.Context()
		vars := mux.Vars(r)
		datasetID := vars["dataset_id"]
		edition := vars["edition"]
		version := vars["version"]
		data := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}
		auditParams := common.Params{"dataset_id": datasetID, "edition": edition, "version": version}

		currentVersion, err := d.Datastore.GetVersion(datasetID, edition, version, "")
		if err != nil {
			if err != errs.ErrVersionNotFound {
				log.ErrorCtx(ctx, errors.WithMessage(err, "errored whilst retrieving version resource"), data)

				if auditErr := d.Auditor.Record(ctx, action, audit.Unsuccessful, auditParams); auditErr != nil {
					err = errs.ErrInternalServer
				}

				request.DrainBody(r)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			// If document cannot be found do not handle error
			handle(w, r)
			return
		}

		if currentVersion != nil {
			if currentVersion.State == models.PublishedState {

				// We can allow public download links to be modified by the exporter
				// services when a version is published. Note that a new version will be
				// created which contain only the download information to prevent any
				// forbidden fields from being set on the published version

				// TODO Logic here might require it's own endpoint,
				// possibly /datasets/.../versions/<version>/downloads
				if action == updateVersionAction {
					versionDoc, err := models.CreateVersion(r.Body)
					if err != nil {
						log.ErrorCtx(ctx, errors.WithMessage(err, "failed to model version resource based on request"), data)

						if auditErr := d.Auditor.Record(ctx, action, audit.Unsuccessful, auditParams); auditErr != nil {
							request.DrainBody(r)
							http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
							return
						}

						request.DrainBody(r)
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}

					if versionDoc.Downloads != nil {
						newVersion := &models.Version{Downloads: &models.DownloadList{}}
						if versionDoc.Downloads.CSV != nil && versionDoc.Downloads.CSV.Public != "" {
							newVersion.Downloads.CSV = &models.DownloadObject{
								Public: versionDoc.Downloads.CSV.Public,
								Size:   versionDoc.Downloads.CSV.Size,
								HRef:   versionDoc.Downloads.CSV.HRef,
							}
						}

						if versionDoc.Downloads.CSVW != nil && versionDoc.Downloads.CSVW.Public != "" {
							newVersion.Downloads.CSVW = &models.DownloadObject{
								Public: versionDoc.Downloads.CSVW.Public,
								Size:   versionDoc.Downloads.CSVW.Size,
								HRef:   versionDoc.Downloads.CSVW.HRef,
							}
						}

						if versionDoc.Downloads.XLS != nil && versionDoc.Downloads.XLS.Public != "" {
							newVersion.Downloads.XLS = &models.DownloadObject{
								Public: versionDoc.Downloads.XLS.Public,
								Size:   versionDoc.Downloads.XLS.Size,
								HRef:   versionDoc.Downloads.XLS.HRef,
							}
						}

						if newVersion != nil {
							var b []byte
							b, err = json.Marshal(newVersion)
							if err != nil {
								log.ErrorCtx(ctx, errors.WithMessage(err, "failed to marshal new version resource based on request"), data)

								if auditErr := d.Auditor.Record(ctx, action, audit.Unsuccessful, auditParams); auditErr != nil {
									request.DrainBody(r)
									http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
									return
								}

								request.DrainBody(r)
								http.Error(w, err.Error(), http.StatusForbidden)
								return
							}

							if err = r.Body.Close(); err != nil {
								log.ErrorCtx(ctx, errors.WithMessage(err, "could not close response body"), data)
							}

							// Set variable `has_downloads` to true to prevent request
							// triggering version from being republished
							vars[hasDownloads] = trueStringified
							r.Body = ioutil.NopCloser(bytes.NewBuffer(b))
							handle(w, r)
							return
						}
					}
				}

				err = errors.New("unable to update version as it has been published")
				data["version"] = currentVersion
				log.ErrorCtx(ctx, err, data)
				if auditErr := d.Auditor.Record(ctx, action, audit.Unsuccessful, auditParams); auditErr != nil {
					request.DrainBody(r)
					http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
					return
				}

				request.DrainBody(r)
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
		}

		handle(w, r)
	})
}

func setJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}

func (api *DatasetAPI) authenticate(r *http.Request, logData map[string]interface{}) (bool, map[string]interface{}) {
	var authorised bool

	if api.EnablePrePublishView {
		var hasCallerIdentity, hasUserIdentity bool

		callerIdentity := common.Caller(r.Context())
		if callerIdentity != "" {
			logData["caller_identity"] = callerIdentity
			hasCallerIdentity = true
		}

		userIdentity := common.User(r.Context())
		if userIdentity != "" {
			logData["user_identity"] = userIdentity
			hasUserIdentity = true
		}

		if hasCallerIdentity || hasUserIdentity {
			authorised = true
		}
		logData["authenticated"] = authorised

		return authorised, logData
	}
	return authorised, logData
}

// Close represents the graceful shutting down of the http server
func Close(ctx context.Context) error {
	if err := httpServer.Shutdown(ctx); err != nil {
		return err
	}
	log.Info("graceful shutdown of http server complete", nil)
	return nil
}
