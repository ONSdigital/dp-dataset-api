package api

import "github.com/gorilla/mux"

// DatasetAPI manages importing filters against a dataset
type DatasetAPI struct {
	dataStore     DataStore
	host          string
	internalToken string
	router        *mux.Router
}

// CreateDatasetAPI manages all the routes configured to API
func CreateDatasetAPI(host string, secretKey string, router *mux.Router, dataStore DataStore) *DatasetAPI {
	router.Path("/healthcheck").Methods("GET").HandlerFunc(healthCheck)

	api := DatasetAPI{host: host, internalToken: secretKey, dataStore: dataStore, router: router}
	api.router.HandleFunc("/datasets", api.getDatasets).Methods("GET")
	api.router.HandleFunc("/datasets", api.addDataset).Methods("POST")
	api.router.HandleFunc("/datasets/{id}", api.getDataset).Methods("GET")
	api.router.HandleFunc("/datasets/{id}", api.putDataset).Methods("PUT")
	api.router.HandleFunc("/datasets/{id}/editions", api.getEditions).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}", api.getEdition).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}", api.addEdition).Methods("POST")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions", api.getVersions).Methods("GET")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions", api.addVersion).Methods("POST")
	api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}", api.getVersion).Methods("GET")
	//api.router.HandleFunc("/datasets/{id}/editions/{edition}/versions/{version}", api.putVersion).Methods("PUT")

	return &api
}
