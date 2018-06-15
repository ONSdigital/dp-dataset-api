package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
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
	"github.com/ONSdigital/go-ns/server"
	"github.com/gorilla/mux"
	"github.com/justinas/alice"
	"github.com/pkg/errors"
)

var httpServer *server.Server

const (
	datasetDocType         = "dataset"
	editionDocType         = "edition"
	versionDocType         = "version"
	observationDocType     = "observation"
	downloadServiceToken   = "X-Download-Service-Token"
	dimensionDocType       = "dimension"
	dimensionOptionDocType = "dimension-option"

	// audit actions
	getDatasetsAction      = "getDatasets"
	getDatasetAction       = "getDataset"
	putDatasetAction       = "putDataset"
	getEditionsAction      = "getEditions"
	getEditionAction       = "getEdition"
	getVersionsAction      = "getVersions"
	getVersionAction       = "getVersion"
	updateVersionAction    = "updateVersion"
	publishVersionAction   = "publishVersion"
	associateVersionAction = "associateVersionAction"
	deleteDatasetAction    = "deleteDataset"
	addDatasetAction       = "addDataset"
	getDimensionsAction    = "getDimensions"
	getMetadataAction      = "getMetadata"

	auditError     = "error while attempting to record audit event, failing request"
	auditActionErr = "failed to audit action"
)

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
	observationStore     ObservationStore
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

func setJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}

// CreateDatasetAPI manages all the routes configured to API
func CreateDatasetAPI(cfg config.Configuration, dataStore store.DataStore, urlBuilder *url.Builder, errorChan chan error, downloadsGenerator DownloadsGenerator, auditor Auditor, observationStore ObservationStore) {
	router := mux.NewRouter()
	Routes(cfg, router, dataStore, urlBuilder, downloadsGenerator, auditor, observationStore)

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
func Routes(cfg config.Configuration, router *mux.Router, dataStore store.DataStore, urlBuilder *url.Builder, downloadGenerator DownloadsGenerator, auditor Auditor, observationStore ObservationStore) *DatasetAPI {

	api := DatasetAPI{
		dataStore:            dataStore,
		observationStore:     observationStore,
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
	api.Router.HandleFunc("/datasets/{id}", api.getDataset).Methods("GET")
	api.Router.HandleFunc("/datasets/{id}/editions", api.getEditions).Methods("GET")
	api.Router.HandleFunc("/datasets/{id}/editions/{edition}", api.getEdition).Methods("GET")
	api.Router.HandleFunc("/datasets/{id}/editions/{edition}/versions", api.getVersions).Methods("GET")
	api.Router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}", api.getVersion).Methods("GET")
	api.Router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}/metadata", api.getMetadata).Methods("GET")
	api.Router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}/observations", api.getObservations).Methods("GET")
	api.Router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}/dimensions", api.getDimensions).Methods("GET")
	api.Router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}/dimensions/{dimension}/options", api.getDimensionOptions).Methods("GET")

	if cfg.EnablePrivateEnpoints {

		log.Debug("private endpoints have been enabled", nil)

		versionPublishChecker := PublishCheck{Datastore: dataStore.Backend}
		api.Router.HandleFunc("/datasets/{id}", identity.Check(api.addDataset)).Methods("POST")
		api.Router.HandleFunc("/datasets/{id}", identity.Check(api.putDataset)).Methods("PUT")
		api.Router.HandleFunc("/datasets/{id}", identity.Check(api.deleteDataset)).Methods("DELETE")
		api.Router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}", identity.Check(versionPublishChecker.Check(api.putVersion))).Methods("PUT")

		instanceAPI := instance.Store{Host: api.host, Storer: api.dataStore.Backend, Auditor: auditor}
		instancePublishChecker := instance.PublishCheck{Auditor: auditor, Datastore: dataStore.Backend}
		api.Router.HandleFunc("/instances", identity.Check(instanceAPI.GetList)).Methods("GET")
		api.Router.HandleFunc("/instances", identity.Check(instanceAPI.Add)).Methods("POST")
		api.Router.HandleFunc("/instances/{id}", identity.Check(instanceAPI.Get)).Methods("GET")
		api.Router.HandleFunc("/instances/{id}", identity.Check(instancePublishChecker.Check(instanceAPI.Update, instance.PutInstanceAction))).Methods("PUT")
		api.Router.HandleFunc("/instances/{id}/dimensions/{dimension}", identity.Check(instancePublishChecker.Check(instanceAPI.UpdateDimension, instance.PutDimensionAction))).Methods("PUT")
		api.Router.HandleFunc("/instances/{id}/events", identity.Check(instanceAPI.AddEvent)).Methods("POST")
		api.Router.HandleFunc("/instances/{id}/inserted_observations/{inserted_observations}",
			identity.Check(instancePublishChecker.Check(instanceAPI.UpdateObservations, instance.PutInsertedObservations))).Methods("PUT")
		api.Router.HandleFunc("/instances/{id}/import_tasks", identity.Check(instancePublishChecker.Check(instanceAPI.UpdateImportTask, instance.PutImportTasks))).Methods("PUT")

		dimensionAPI := dimension.Store{Auditor: auditor, Storer: api.dataStore.Backend}
		api.Router.HandleFunc("/instances/{id}/dimensions", identity.Check(dimensionAPI.GetNodesHandler)).Methods("GET")
		api.Router.HandleFunc("/instances/{id}/dimensions", identity.Check(instancePublishChecker.Check(dimensionAPI.AddHandler, dimension.PostDimensionsAction))).Methods("POST")
		api.Router.HandleFunc("/instances/{id}/dimensions/{dimension}/options", identity.Check(dimensionAPI.GetUniqueHandler)).Methods("GET")
		api.Router.HandleFunc("/instances/{id}/dimensions/{dimension}/options/{value}/node_id/{node_id}",
			identity.Check(instancePublishChecker.Check(dimensionAPI.AddNodeIDHandler, dimension.PutNodeIDAction))).Methods("PUT")
	}
	return &api
}

func handleErrorType(docType string, err error, w http.ResponseWriter) {
	log.Error(err, nil)

	switch docType {
	default:
		if err == errs.ErrEditionNotFound || err == errs.ErrVersionNotFound || err == errs.ErrDimensionNodeNotFound || err == errs.ErrInstanceNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	case "dimension":
		if err == errs.ErrDatasetNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else if err == errs.ErrEditionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else if err == errs.ErrVersionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else if err == errs.ErrDimensionsNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// Check wraps a HTTP handle. Checks that the state is not published
func (d *PublishCheck) Check(handle func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		vars := mux.Vars(r)
		id := vars["id"]
		edition := vars["edition"]
		version := vars["version"]

		currentVersion, err := d.Datastore.GetVersion(id, edition, version, "")
		if err != nil {
			if err != errs.ErrVersionNotFound {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			// If document cannot be found do not handle error
			handle(w, r)
			return
		}

		if currentVersion != nil {
			if currentVersion.State == models.PublishedState {
				defer func() {
					if err := r.Body.Close(); err != nil {
						log.ErrorC("could not close response body", err, nil)
					}
				}()

				versionDoc, err := models.CreateVersion(r.Body)
				if err != nil {
					log.ErrorC("failed to model version resource based on request", err, log.Data{"dataset_id": id, "edition": edition, "version": version})
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}

				// We can allow public download links to be modified by the exporters when a version is published.
				// Note that a new version will be created which contain only the download information to prevent
				// any forbidden fields from being set on the published version
				if r.Method == "PUT" {
					if versionDoc.Downloads != nil {
						newVersion := new(models.Version)
						if versionDoc.Downloads.CSV != nil && versionDoc.Downloads.CSV.Public != "" {
							newVersion = &models.Version{
								Downloads: &models.DownloadList{
									CSV: &models.DownloadObject{
										Public: versionDoc.Downloads.CSV.Public,
										Size:   versionDoc.Downloads.CSV.Size,
										HRef:   versionDoc.Downloads.CSV.HRef,
									},
								},
							}
						}
						if versionDoc.Downloads.XLS != nil && versionDoc.Downloads.XLS.Public != "" {
							newVersion = &models.Version{
								Downloads: &models.DownloadList{
									XLS: &models.DownloadObject{
										Public: versionDoc.Downloads.XLS.Public,
										Size:   versionDoc.Downloads.XLS.Size,
										HRef:   versionDoc.Downloads.XLS.HRef,
									},
								},
							}
						}
						if newVersion != nil {
							b, err := json.Marshal(newVersion)
							if err != nil {
								http.Error(w, err.Error(), http.StatusForbidden)
								return
							}

							if err := r.Body.Close(); err != nil {
								log.ErrorC("could not close response body", err, nil)
							}
							r.Body = ioutil.NopCloser(bytes.NewBuffer(b))
							handle(w, r)
							return
						}
					}
				}

				err = errors.New("unable to update version as it has been published")
				log.Error(err, log.Data{"version": currentVersion})
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
		}

		handle(w, r)
	})
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

func handleAuditingFailure(w http.ResponseWriter, err error, logData log.Data) {
	log.ErrorC(auditError, err, logData)
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func auditActionFailure(ctx context.Context, auditedAction string, auditedResult string, err error, logData log.Data) {
	if logData == nil {
		logData = log.Data{}
	}

	logData["auditAction"] = auditedAction
	logData["auditResult"] = auditedResult

	logError(ctx, errors.WithMessage(err, auditActionErr), logData)
}

func logError(ctx context.Context, err error, data log.Data) {
	if data == nil {
		data = log.Data{}
	}
	reqID := common.GetRequestId(ctx)
	if user := common.User(ctx); user != "" {
		data["user"] = user
	}
	if caller := common.Caller(ctx); caller != "" {
		data["caller"] = caller
	}
	log.ErrorC(reqID, err, data)
}

func logInfo(ctx context.Context, message string, data log.Data) {
	if data == nil {
		data = log.Data{}
	}
	reqID := common.GetRequestId(ctx)
	if user := common.User(ctx); user != "" {
		data["user"] = user
	}
	if caller := common.Caller(ctx); caller != "" {
		data["caller"] = caller
	}
	log.InfoC(reqID, message, data)
}

// Close represents the graceful shutting down of the http server
func Close(ctx context.Context) error {
	if err := httpServer.Shutdown(ctx); err != nil {
		return err
	}
	log.Info("graceful shutdown of http server complete", nil)
	return nil
}
