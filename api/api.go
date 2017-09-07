package api

import (
	"github.com/ONSdigital/dp-dataset-api/auth"
	"github.com/ONSdigital/dp-dataset-api/dimension"
	"github.com/ONSdigital/dp-dataset-api/instance"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/gorilla/mux"
)

// DatasetAPI manages importing filters against a dataset
type DatasetAPI struct {
	DataStore   store.DataStore
	Router      mux.Router
	PrivateAuth *auth.Authenticator
}

//go:generate moq -out apitest/api.go -pkg apitest . API
type API interface {
	CreateDatasetAPI(string, *mux.Router, store.DataStore) *DatasetAPI
}

// CreateDatasetAPI manages all the routes configured to API
func CreateDatasetAPI(secretKey string, router *mux.Router, dataStore store.DataStore) *DatasetAPI {
	router.Path("/healthcheck").Methods("GET").HandlerFunc(healthCheck)

	api := DatasetAPI{PrivateAuth: &auth.Authenticator{secretKey, "internal-token"}, DataStore: dataStore, Router: *router}
	api.Router.HandleFunc("/datasets", api.getDatasets).Methods("GET")
	api.Router.HandleFunc("/datasets", api.addDataset).Methods("POST")
	api.Router.HandleFunc("/datasets/{id}", api.getDataset).Methods("GET")
	api.Router.HandleFunc("/datasets/{id}/editions", api.getEditions).Methods("GET")
	api.Router.HandleFunc("/datasets/{id}/editions/{edition}", api.getEdition).Methods("GET")
	api.Router.HandleFunc("/datasets/{id}/editions/{edition}", api.addEdition).Methods("POST")
	api.Router.HandleFunc("/datasets/{id}/editions/{edition}/versions", api.getVersions).Methods("GET")
	api.Router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}", api.getVersion).Methods("GET")
	api.Router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}", api.addVersion).Methods("POST")

	instance := instance.Store{api.DataStore.Backend}
	dimension := dimension.Store{api.DataStore.Backend}
	api.Router.HandleFunc("/instances", instance.GetList).Methods("GET")
	api.Router.HandleFunc("/instances", api.PrivateAuth.Check(instance.Add)).Methods("POST")
	api.Router.HandleFunc("/instances/{id}", instance.Get).Methods("GET")
	api.Router.HandleFunc("/instances/{id}", api.PrivateAuth.Check(instance.Update)).Methods("PUT")
	api.Router.HandleFunc("/instances/{id}/events", api.PrivateAuth.Check(instance.AddEvent)).Methods("POST")
	api.Router.HandleFunc("/instances/{id}/dimensions", api.PrivateAuth.Check(dimension.GetNodes)).Methods("GET")
	api.Router.HandleFunc("/instances/{id}/dimensions/{dimension}/options", dimension.GetUnique).Methods("GET")
	api.Router.HandleFunc("/instances/{id}/dimensions/{dimension}/options/{value}", api.PrivateAuth.Check(instance.AddDimension)).Methods("PUT")
	api.Router.HandleFunc("/instances/{id}/dimensions/{dimension}/options/{value}/node_id/{node_id}", api.PrivateAuth.Check(dimension.AddNodeID)).Methods("PUT")
	api.Router.HandleFunc("/instances/{id}/inserted_observations/{inserted_observations}", api.PrivateAuth.Check(instance.UpdateObservations)).Methods("PUT")
	return &api
}
