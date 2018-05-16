package api

import (
	"encoding/json"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
)

func (api *DatasetAPI) getMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	// get dataset document
	datasetDoc, err := api.dataStore.Backend.GetDataset(datasetID)
	if err != nil {
		log.Error(err, logData)
		handleErrorType(versionDocType, err, w)
		return
	}

	authorised, logData := api.authenticate(r, logData)

	var state string

	// if request is authenticated then access resources of state other than published
	if !authorised {
		// Check for current sub document
		if datasetDoc.Current == nil || datasetDoc.Current.State != models.PublishedState {
			log.ErrorC("found dataset but currently unpublished", errs.ErrDatasetNotFound, log.Data{"dataset_id": datasetID, "edition": edition, "version": version, "dataset": datasetDoc.Current})
			http.Error(w, errs.ErrDatasetNotFound.Error(), http.StatusNotFound)
			return
		}

		state = datasetDoc.Current.State
	}

	if err = api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
		log.ErrorC("failed to find edition for dataset", err, logData)
		handleErrorType(versionDocType, err, w)
		return
	}

	versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, state)
	if err != nil {
		log.ErrorC("failed to find version for dataset edition", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	if err = models.CheckState("version", versionDoc.State); err != nil {
		log.ErrorC("unpublished version has an invalid state", err, log.Data{"state": versionDoc.State})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var metaDataDoc *models.Metadata
	// combine version and dataset metadata
	if state != models.PublishedState {
		metaDataDoc = models.CreateMetaDataDoc(datasetDoc.Next, versionDoc, api.urlBuilder)
	} else {
		metaDataDoc = models.CreateMetaDataDoc(datasetDoc.Current, versionDoc, api.urlBuilder)
	}

	b, err := json.Marshal(metaDataDoc)
	if err != nil {
		log.ErrorC("failed to marshal metadata resource into bytes", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Debug("get metadata relevant to version", logData)
}
