package api

import (
	"context"
	"time"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/dimension"
	"github.com/ONSdigital/dp-dataset-api/instance"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/ONSdigital/go-ns/identity"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/server"
	"github.com/gorilla/mux"
	"github.com/justinas/alice"
)

var httpServer *server.Server

//API provides an interface for the routes
type API interface {
	CreateDatasetAPI(string, *mux.Router, store.DataStore) *DatasetAPI
}

// DownloadsGenerator pre generates full file downloads for the specified dataset/edition/version
type DownloadsGenerator interface {
	Generate(datasetID, instanceID, edition, version string) error
}

// DatasetAPI manages importing filters against a dataset
type DatasetAPI struct {
	dataStore            store.DataStore
	host                 string
	zebedeeURL           string
	internalToken        string
	downloadServiceToken string
	EnablePrePublishView bool
	router               *mux.Router
	urlBuilder           *url.Builder
	downloadGenerator    DownloadsGenerator
	healthCheckTimeout   time.Duration
	serviceAuthToken     string
}

// CreateDatasetAPI manages all the routes configured to API
func CreateDatasetAPI(cfg config.Configuration, dataStore store.DataStore, urlBuilder *url.Builder, errorChan chan error, downloadsGenerator DownloadsGenerator) {
	router := mux.NewRouter()
	routes(cfg, router, dataStore, urlBuilder, downloadsGenerator)

	alice := alice.New(identity.Handler(true, cfg.ZebedeeURL)).Then(router)
	httpServer = server.New(cfg.BindAddr, alice)
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

func routes(cfg config.Configuration, router *mux.Router, dataStore store.DataStore, urlBuilder *url.Builder, downloadGenerator DownloadsGenerator) *DatasetAPI {

	api := DatasetAPI{
		dataStore:            dataStore,
		host:                 cfg.DatasetAPIURL,
		zebedeeURL:           cfg.ZebedeeURL,
		serviceAuthToken:     cfg.ServiceAuthToken,
		downloadServiceToken: cfg.DownloadServiceSecretKey,
		EnablePrePublishView: cfg.EnablePrivateEnpoints,
		router:               router,
		urlBuilder:           urlBuilder,
		downloadGenerator:    downloadGenerator,
		healthCheckTimeout:   cfg.HealthCheckTimeout,
	}

	api.router.HandleFunc("/healthcheck", api.healthCheck).Methods("GET")

	api.router.HandleFunc("/datasets", api.getDatasets).Methods("GET")
	api.router.HandleFunc("/datasets/{id}", api.getDataset).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions", api.getEditions).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}", api.getEdition).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions", api.getVersions).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}", api.getVersion).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}/metadata", api.getMetadata).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}/dimensions", api.getDimensions).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}/dimensions/{dimension}/options", api.getDimensionOptions).Methods("GET")

	if cfg.EnablePrivateEnpoints {

		log.Debug("private endpoints have been enabled", nil)

		versionPublishChecker := PublishCheck{Datastore: dataStore.Backend}
		api.router.HandleFunc("/datasets/{id}", identity.Check(api.addDataset)).Methods("POST")
		api.router.HandleFunc("/datasets/{id}", identity.Check(api.putDataset)).Methods("PUT")
		api.router.HandleFunc("/datasets/{id}", identity.Check(api.deleteDataset)).Methods("DELETE")
		api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}", identity.Check(versionPublishChecker.Check(api.putVersion))).Methods("PUT")

		instanceAPI := instance.Store{Host: api.host, Storer: api.dataStore.Backend}
		instancePublishChecker := instance.PublishCheck{Datastore: dataStore.Backend}
		api.router.HandleFunc("/instances", identity.Check(instanceAPI.GetList)).Methods("GET")
		api.router.HandleFunc("/instances", identity.Check(instanceAPI.Add)).Methods("POST")
		api.router.HandleFunc("/instances/{id}", identity.Check(instanceAPI.Get)).Methods("GET")
		api.router.HandleFunc("/instances/{id}", identity.Check(instancePublishChecker.Check(instanceAPI.Update))).Methods("PUT")
		api.router.HandleFunc("/instances/{id}/dimensions/{dimension}", identity.Check(instancePublishChecker.Check(instanceAPI.UpdateDimension))).Methods("PUT")
		api.router.HandleFunc("/instances/{id}/events", identity.Check(instanceAPI.AddEvent)).Methods("POST")
		api.router.HandleFunc("/instances/{id}/inserted_observations/{inserted_observations}",
			identity.Check(instancePublishChecker.Check(instanceAPI.UpdateObservations))).Methods("PUT")
		api.router.HandleFunc("/instances/{id}/import_tasks", identity.Check(instancePublishChecker.Check(instanceAPI.UpdateImportTask))).Methods("PUT")

		dimension := dimension.Store{Storer: api.dataStore.Backend}
		api.router.HandleFunc("/instances/{id}/dimensions", identity.Check(dimension.GetNodes)).Methods("GET")
		api.router.HandleFunc("/instances/{id}/dimensions", identity.Check(instancePublishChecker.Check(dimension.Add))).Methods("POST")
		api.router.HandleFunc("/instances/{id}/dimensions/{dimension}/options", identity.Check(dimension.GetUnique)).Methods("GET")
		api.router.HandleFunc("/instances/{id}/dimensions/{dimension}/options/{value}/node_id/{node_id}",
			identity.Check(instancePublishChecker.Check(dimension.AddNodeID))).Methods("PUT")
	}
	return &api
}

// Close represents the graceful shutting down of the http server
func Close(ctx context.Context) error {
	if err := httpServer.Shutdown(ctx); err != nil {
		return err
	}
	log.Info("graceful shutdown of http server complete", nil)
	return nil
}
