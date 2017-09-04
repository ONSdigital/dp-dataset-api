package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/ONSdigital/dp-dataset-api/api-errors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"gopkg.in/mgo.v2/bson"
)

const state = "published"

func (api *DatasetAPI) getDatasets(w http.ResponseWriter, r *http.Request) {
	results, err := api.dataStore.Backend.GetDatasets()
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		log.Error(err, nil)
		handleErrorType(err, w)
		return
	}
	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug("get all datasets", nil)
}

func (api *DatasetAPI) getDataset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	dataset, err := api.dataStore.Backend.GetDataset(id)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id})
		handleErrorType(err, w)
		return
	}

	bytes, err := json.Marshal(dataset)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id})
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug("get dataset", log.Data{"dataset_id": id})
}

func (api *DatasetAPI) getEditions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	results, err := api.dataStore.Backend.GetEditions(id)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id})
		handleErrorType(err, w)
		return
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id})
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug("get all editions", log.Data{"dataset_id": id})
}

func (api *DatasetAPI) getEdition(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	editionID := vars["edition"]
	edition, err := api.dataStore.Backend.GetEdition(id, editionID)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID})
		handleErrorType(err, w)
		return
	}

	bytes, err := json.Marshal(edition)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID})
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug("get edition", log.Data{"dataset_id": id, "edition": editionID})
}

func (api *DatasetAPI) getVersions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	editionID := vars["edition"]
	results, err := api.dataStore.Backend.GetVersions(id, editionID)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID})
		handleErrorType(err, w)
		return
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID})
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug("get all versions", log.Data{"dataset_id": id, "edition": editionID})
}

func (api *DatasetAPI) getVersion(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	editionID := vars["edition"]
	version := vars["version"]
	results, err := api.dataStore.Backend.GetVersion(id, editionID, version)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID, "version": version})
		handleErrorType(err, w)
		return
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID, "version": version})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID, "version": version})
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug("get version", log.Data{"dataset_id": id, "edition": editionID, "version": version})
}

func (api *DatasetAPI) addDataset(w http.ResponseWriter, r *http.Request) {
	dataset, err := models.CreateDataset(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	datasetID := dataset.ID
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
	log.Debug("upsert dataset", log.Data{"dataset_id": datasetID})
}

func (api *DatasetAPI) addEdition(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	editionID := vars["edition"]

	edition, err := models.CreateEdition(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	edition.Links.Dataset.ID = datasetID
	edition.Links.Dataset.Link = "/datasets/" + datasetID
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
	log.Debug("upsert edition", log.Data{"dataset_id": datasetID, "edition": editionID})
}

func (api *DatasetAPI) addVersion(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	editionID := vars["edition"]

	version, err := models.CreateVersion(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	nextVersion, err := api.dataStore.Backend.GetNextVersion(datasetID, editionID)
	if err != nil {
		log.ErrorR(r, err, nil)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	versionID := nextVersion
	version.Links.Dataset.ID = datasetID
	version.Links.Dataset.Link = "/datasets/" + datasetID
	version.Links.Edition.Link = editionID
	version.Links.Edition.Link = "/datasets/" + datasetID + "/editions/" + editionID
	version.Links.Self = "/datasets/" + datasetID + "/editions/" + editionID + "/versions/" + strconv.Itoa(versionID)
	version.Links.Dimensions = "/instance/" + strconv.Itoa(versionID) + "/dimensions"

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

	if version.State == state {
		updateDataset := bson.M{
			"$set": bson.M{
				"links.latest_version.link": version.Links.Self,
				"links.latest_version.id":   version.ID,
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

	log.Debug("upsert version", log.Data{"dataset_id": datasetID, "edition": editionID, "version": version})
}

func handleErrorType(err error, w http.ResponseWriter) {
	if err == api_errors.DatasetNotFound || err == api_errors.EditionNotFound || err == api_errors.VersionNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
	} else {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func setJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}
