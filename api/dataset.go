package api

import (
	"encoding/json"
	"net/http"
	"github.com/gorilla/mux"
)

func (api *DatasetAPI) getDatasets(w http.ResponseWriter, r *http.Request) {
	results, err := api.dataStore.GetDatasets()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (api *DatasetAPI) getDataset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	dataset, err := api.dataStore.GetDataset(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	bytes, err := json.Marshal(dataset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

}

func (api *DatasetAPI) getEditions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	results, err := api.dataStore.GetEditions(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (api *DatasetAPI) getEdition(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	editionID := vars["edition"]
	edition, err := api.dataStore.GetEdition(id, editionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	bytes, err := json.Marshal(edition)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (api *DatasetAPI) getVersions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	editionID := vars["edition"]
	results, err := api.dataStore.GetVersions(id, editionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (api *DatasetAPI) getVersion(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	editionID := vars["edition"]
	version := vars["version"]
	results, err := api.dataStore.GetVersion(id, editionID, version)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func setJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}
