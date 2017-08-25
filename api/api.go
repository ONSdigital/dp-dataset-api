package api

import "github.com/gorilla/mux"

// DatasetAPI manages importing filters against a dataset
type DatasetAPI struct {
	dataStore     DataStore
	internalToken string
	router        *mux.Router
}

// CreateDatasetAPI manages all the routes configured to API
func CreateDatasetAPI(secretKey string, router *mux.Router, dataStore DataStore) *DatasetAPI {
	router.Path("/healthcheck").Methods("GET").HandlerFunc(healthCheck)

	api := DatasetAPI{internalToken: secretKey, dataStore: dataStore, router: router}
	api.router.HandleFunc("/datasets", api.getDatasets).Methods("GET")
	api.router.HandleFunc("/datasets/{id}", api.addDataset).Methods("POST")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}", api.addEdition).Methods("POST")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}", api.addVersion).Methods("POST")
	return &api
}
