package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"gopkg.in/mgo.v2/bson"
)

const publishedState = "published"

func (api *DatasetAPI) getDatasets(w http.ResponseWriter, r *http.Request) {
	results, err := api.dataStore.Backend.GetAllDatasets()
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

func (api *DatasetAPI) addDataset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]

	dataset, err := models.CreateDataset(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	defer r.Body.Close()

	dataset.DatasetID = datasetID
	dataset.Links.Self = "/datasets/" + datasetID
	dataset.Links.Editions = "/datasets/" + datasetID + "/editions"

	update := bson.M{
		"$set": dataset,
		"$setOnInsert": bson.M{
			"updated_at": time.Now(),
		},
	}

	if err := api.dataStore.Backend.UpsertDataset(datasetID, update); err != nil {
		log.ErrorR(r, err, nil)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusCreated)
}

func (api *DatasetAPI) addEdition(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	editionID := vars["edition"]

	edition, err := models.CreateEdition(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	defer r.Body.Close()

	edition.Links.Dataset = "/datasets/" + datasetID
	edition.Links.Self = "/datasets/" + datasetID + "/editions/" + editionID
	edition.Links.Versions = "/datasets/" + datasetID + "/editions/" + editionID + "/versions"

	update := bson.M{
		"$set": edition,
		"$setOnInsert": bson.M{
			"updated_at": time.Now(),
		},
	}

	if err := api.dataStore.Backend.UpsertEdition(edition.ID, update); err != nil {
		log.ErrorR(r, err, nil)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusCreated)
}

func (api *DatasetAPI) addVersion(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	editionID := vars["edition"]
	versionID := vars["version"]

	version, err := models.CreateVersion(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	defer r.Body.Close()

	version.Links.Dataset = "/datasets/" + datasetID
	version.Links.Edition = "/datasets/" + datasetID + "/editions/" + editionID
	version.Links.Self = "/datasets/" + datasetID + "/editions/" + editionID + "/versions/" + versionID
	version.Links.Dimensions = "/instance/" + versionID + "/dimensions"

	update := bson.M{
		"$set": version,
		"$setOnInsert": bson.M{
			"updated_at": time.Now(),
		},
	}

	if err := api.dataStore.Backend.UpsertVersion(version.ID, update); err != nil {
		log.ErrorR(r, err, nil)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if version.State == publishedState {
		updateDataset := bson.M{
			"$set": bson.M{
				"links.latest_version": version.Links.Self,
			},
			"$setOnInsert": bson.M{
				"updated_at": time.Now(),
			},
		}

		if err := api.dataStore.Backend.UpsertDataset(datasetID, updateDataset); err != nil {
			log.ErrorC("failed to update dataset document with link to latest version", err, nil)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusCreated)
}

func setJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}
