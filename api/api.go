package api

import "github.com/gorilla/mux"


// DatasetAPI manages importing filters against a dataset
type DatasetAPI struct {
	dataStore   DataStore
	router      *mux.Router
	privateAuth Authenticator
}

// CreateDatasetAPI manages all the routes configured to API
func CreateDatasetAPI(secretKey string, router *mux.Router, dataStore DataStore) *DatasetAPI {
	router.Path("/healthcheck").Methods("GET").HandlerFunc(healthCheck)

	api := DatasetAPI{privateAuth: NewAuthenticator(secretKey, "internal-token"), dataStore: dataStore, router: router}
	api.router.HandleFunc("/datasets", api.getDatasets).Methods("GET")
	api.router.HandleFunc("/datasets", api.addDataset).Methods("POST")
	api.router.HandleFunc("/datasets/{id}", api.getDataset).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions", api.getEditions).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}", api.getEdition).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}", api.addEdition).Methods("POST")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions", api.getVersions).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}", api.getVersion).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}", api.addVersion).Methods("POST")

	api.router.HandleFunc("/instances", api.getInstances).Methods("GET")
	api.router.HandleFunc("/instances",  api.privateAuth.Check(api.addInstance)).Methods("POST")
	api.router.HandleFunc("/instances/{id}", api.getInstance).Methods("GET")
	api.router.HandleFunc("/instances/{id}", api.privateAuth.Check(api.updateInstance)).Methods("PUT")
	api.router.HandleFunc("/instances/{id}/events", api.privateAuth.Check(api.addEventToInstance)).Methods("POST")
	api.router.HandleFunc("/instances/{id}/dimensions", api.privateAuth.Check(api.getDimensionNodes)).Methods("GET")
	api.router.HandleFunc("/instances/{id}/dimensions/{dimension}/options", api.getUniqueDimensions).Methods("GET")
	api.router.HandleFunc("/instances/{id}/dimensions/{dimension}/options/{value}", api.privateAuth.Check(api.addDimensionToInstance)).Methods("PUT")
	api.router.HandleFunc("/instances/{id}/dimensions/{dimension}/options/{value}/node_id/{node_id}", api.privateAuth.Check(api.addNodeIdToDimension)).Methods("PUT")
	api.router.HandleFunc("/instances/{id}/inserted_observations/{inserted_observations}", api.privateAuth.Check(api.updateObservations)).Methods("PUT")
	return &api
}
