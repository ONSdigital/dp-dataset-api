package api

import (
	"context"

	"github.com/ONSdigital/dp-dataset-api/auth"
	"github.com/ONSdigital/dp-dataset-api/dimension"
	"github.com/ONSdigital/dp-dataset-api/instance"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/server"
	"github.com/gorilla/mux"
)

//go:generate moq -out ../mocks/mocks.go -pkg mocks . DownloadsGenerator

var httpServer *server.Server

//API provides an interface for the routes
type API interface {
	CreateDatasetAPI(string, *mux.Router, store.DataStore) *DatasetAPI
}

// DownloadsGenerator pre generates full file downloads for the specified dataset/edition/version
type DownloadsGenerator interface {
	Generate(datasetID string, instanceID string, edition string, version string) error
}

// DatasetAPI manages importing filters against a dataset
type DatasetAPI struct {
	dataStore         store.DataStore
	host              string
	internalToken     string
	privateAuth       *auth.Authenticator
	router            *mux.Router
	downloadGenerator DownloadsGenerator
}

// CreateDatasetAPI manages all the routes configured to API
func CreateDatasetAPI(host, bindAddr, secretKey string, dataStore store.DataStore, errorChan chan error, downloadsGenerator DownloadsGenerator) {
	router := mux.NewRouter()
	routes(host, secretKey, router, dataStore, downloadsGenerator)

	httpServer = server.New(bindAddr, router)
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

func routes(host, secretKey string, router *mux.Router, dataStore store.DataStore, downloadGenerator DownloadsGenerator) *DatasetAPI {
	api := DatasetAPI{
		privateAuth: &auth.Authenticator{
			SecretKey:  secretKey,
			HeaderName: "internal-token",
		},
		dataStore:         dataStore,
		host:              host,
		internalToken:     secretKey,
		router:            router,
		downloadGenerator: downloadGenerator,
	}

	router.Path("/healthcheck").Methods("GET").HandlerFunc(api.healthCheck)

	api.router.HandleFunc("/datasets", api.getDatasets).Methods("GET")
	api.router.HandleFunc("/datasets/{id}", api.privateAuth.Check(api.addDataset)).Methods("POST")
	api.router.HandleFunc("/datasets/{id}", api.getDataset).Methods("GET")
	api.router.HandleFunc("/datasets/{id}", api.privateAuth.Check(api.putDataset)).Methods("PUT")
	api.router.HandleFunc("/datasets/{id}/editions", api.getEditions).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}", api.getEdition).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions", api.getVersions).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}", api.getVersion).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}", api.privateAuth.Check(api.putVersion)).Methods("PUT")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}/downloads", api.privateAuth.Check(api.PutVersionDownloads)).Methods("PUT")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}/metadata", api.getMetadata).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}/dimensions", api.getDimensions).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}/dimensions/{dimension}/options", api.getDimensionOptions).Methods("GET")

	instance := instance.Store{Host: api.host, Storer: api.dataStore.Backend}
	api.router.HandleFunc("/instances", api.privateAuth.Check(instance.GetList)).Methods("GET")
	api.router.HandleFunc("/instances", api.privateAuth.Check(instance.Add)).Methods("POST")
	api.router.HandleFunc("/instances/{id}", api.privateAuth.Check(instance.Get)).Methods("GET")
	api.router.HandleFunc("/instances/{id}", api.privateAuth.Check(instance.Update)).Methods("PUT")
	api.router.HandleFunc("/instances/{id}/events", api.privateAuth.Check(instance.AddEvent)).Methods("POST")
	api.router.HandleFunc("/instances/{id}/inserted_observations/{inserted_observations}", api.privateAuth.Check(instance.UpdateObservations)).Methods("PUT")

	dimension := dimension.Store{Storer: api.dataStore.Backend}
	api.router.HandleFunc("/instances/{id}/dimensions", dimension.GetNodes).Methods("GET")
	api.router.HandleFunc("/instances/{id}/dimensions", api.privateAuth.Check(dimension.Add)).Methods("POST")
	api.router.HandleFunc("/instances/{id}/dimensions/{dimension}/options", dimension.GetUnique).Methods("GET")
	api.router.HandleFunc("/instances/{id}/dimensions/{dimension}/options/{value}/node_id/{node_id}", api.privateAuth.Check(dimension.AddNodeID)).Methods("PUT")
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
