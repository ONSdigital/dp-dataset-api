package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ONSdigital/dp-dataset-api/api-errors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
)

const (
	publishedState  = "published"
	associatedState = "associated"

	internalToken = "internal_token"
)

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

	var bytes []byte
	if r.Header.Get(internalToken) != api.internalToken {
		if dataset.Current == nil {
			handleErrorType(api_errors.DatasetNotFound, w)
			return
		}
		bytes, err = json.Marshal(dataset.Current)
		if err != nil {
			log.Error(err, log.Data{"dataset_id": id})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		if dataset == nil {
			handleErrorType(api_errors.DatasetNotFound, w)
		}
		bytes, err = json.Marshal(dataset)
		if err != nil {
			log.Error(err, log.Data{"dataset_id": id})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
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

	var state string
	if r.Header.Get(internalToken) != api.internalToken {
		state = publishedState
	}

	results, err := api.dataStore.Backend.GetEditions(id, state)
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

	var state string
	if r.Header.Get(internalToken) != api.internalToken {
		state = publishedState
	}

	edition, err := api.dataStore.Backend.GetEdition(id, editionID, state)
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

	var state string
	if r.Header.Get(internalToken) != api.internalToken {
		state = publishedState
	}

	results, err := api.dataStore.Backend.GetVersions(id, editionID, state)
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

	var state string
	if r.Header.Get(internalToken) != api.internalToken {
		state = publishedState
	}

	results, err := api.dataStore.Backend.GetVersion(id, editionID, version, state)
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
	if r.Header.Get(internalToken) != api.internalToken {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	dataset, err := models.CreateDataset(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	datasetID := dataset.ID
	dataset.Links.Self.HRef = api.host + "/datasets/" + datasetID
	dataset.Links.Editions.HRef = api.host + "/datasets/" + datasetID + "/editions"
	dataset.LastUpdated = time.Now()

	datasetDoc := &models.DatasetUpdate{
		ID:   datasetID,
		Next: dataset,
	}

	if err := api.dataStore.Backend.UpsertDataset(datasetID, datasetDoc); err != nil {
		log.ErrorR(r, err, nil)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	bytes, err := json.Marshal(datasetDoc)
	if err != nil {
		log.Error(err, log.Data{"new_dataset": datasetID})
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusCreated)
	_, err = w.Write(bytes)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": datasetID})
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug("upsert dataset", log.Data{"dataset_id": datasetID})
}

func (api *DatasetAPI) addEdition(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	edition := vars["edition"]
	if r.Header.Get(internalToken) != api.internalToken {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Check if edition already exists and if it has been published return a status of Forbidden
	currentEdition, err := api.dataStore.Backend.GetEdition(datasetID, edition, "")
	if err != nil {
		if err != api_errors.EditionNotFound {
			log.Error(err, log.Data{"dataset_id": datasetID, "edition": edition})
			handleErrorType(err, w)
			return
		}
	} else {
		if currentEdition.State == publishedState {
			w.WriteHeader(http.StatusForbidden)
			return
		}
	}

	editionDoc, err := models.CreateEdition(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	editionDoc.Edition = edition
	editionDoc.Links.Dataset.ID = datasetID
	editionDoc.Links.Dataset.HRef = api.host + "/datasets/" + datasetID
	editionDoc.Links.Self.HRef = api.host + "/datasets/" + datasetID + "/editions/" + edition
	editionDoc.Links.Versions.HRef = api.host + "/datasets/" + datasetID + "/editions/" + edition + "/versions"

	if err := api.dataStore.Backend.UpsertEdition(edition, editionDoc); err != nil {
		log.ErrorR(r, err, nil)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusCreated)
	log.Debug("upsert edition", log.Data{"dataset_id": datasetID, "edition": edition})
}

func (api *DatasetAPI) addVersion(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	edition := vars["edition"]
	if r.Header.Get(internalToken) != api.internalToken {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	version, err := models.CreateVersion(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err = models.ValidateVersion(version); err != nil {
		log.ErrorR(r, err, nil)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	editionDoc, err := api.dataStore.Backend.GetEdition(datasetID, edition, "")
	if err != nil {
		log.ErrorR(r, err, nil)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	nextVersion, err := api.dataStore.Backend.GetNextVersion(datasetID, edition)
	if err != nil {
		log.ErrorR(r, err, nil)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	versionID := strconv.Itoa(nextVersion)
	api.reviseVersionWithAdditionalFields(*version, editionDoc, datasetID, edition, versionID)

	if err := api.dataStore.Backend.UpsertVersion(version.ID, version); err != nil {
		log.ErrorR(r, err, nil)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if version.State == publishedState {
		if err := api.dataStore.Backend.UpdateEdition(editionDoc.ID, version.State); err != nil {
			log.ErrorC("failed to update the state of edition document to published", err, nil)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if err := api.updateDataset(datasetID, version); err != nil {
			log.ErrorC("failed to update dataset document once version state changes to publish", err, nil)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	if version.State == associatedState {
		if err := api.dataStore.Backend.UpdateDatasetWithAssociation(datasetID, associatedState, version); err != nil {
			log.ErrorC("failed to update dataset document after a version of a dataset has been associated with a collection", err, nil)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusCreated)

	log.Debug("upsert version", log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
}

func (api *DatasetAPI) updateDataset(id string, version *models.Version) error {
	currentDataset, err := api.dataStore.Backend.GetDataset(id)
	if err != nil {
		log.ErrorC("Unable to update dataset", err, log.Data{"dataset_id": id})
		return err
	}

	currentDataset.Next.CollectionID = version.CollectionID
	currentDataset.Next.Links.LatestVersion.ID = version.ID
	currentDataset.Next.Links.LatestVersion.HRef = version.Links.Self.HRef
	currentDataset.Next.State = publishedState
	currentDataset.Next.LastUpdated = time.Now()

	// newDataset.Next will not be cleaned up due to keeping request to mongo
	// idempotent; for instance if an authorised user double clicked to update
	// dataset, the next sub document would not exist to create the correct
	// current sub document on the second click
	newDataset := &models.DatasetUpdate{
		ID:      currentDataset.ID,
		Current: currentDataset.Next,
		Next:    currentDataset.Next,
	}

	if err := api.dataStore.Backend.UpsertDataset(id, newDataset); err != nil {
		log.ErrorC("Unable to update dataset", err, log.Data{"dataset_id": id})
		return err
	}

	return nil
}

func (api *DatasetAPI) reviseVersionWithAdditionalFields(version models.Version, editionDoc *models.Edition, datasetID, edition, versionID string) {
	version.Version = versionID
	version.Edition = edition
	version.Links.Dataset.ID = datasetID
	version.Links.Dataset.HRef = fmt.Sprintf("%s/datasets/%s", api.host, datasetID)
	version.Links.Edition.HRef = fmt.Sprintf("%s/datasets/%s/editions/%s", api.host, datasetID, edition)
	version.Links.Edition.ID = editionDoc.ID
	version.Links.Self.HRef = fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s", api.host, datasetID, edition, versionID)
	version.Links.Dimensions.HRef = fmt.Sprintf("%s/instance/%s/dimensions/", api.host, versionID)
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
