package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/ONSdigital/dp-dataset-api/api-errors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"gopkg.in/mgo.v2/bson"
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

	var selector bson.M

	if r.Header.Get(internalToken) == api.internalToken {
		selector = bson.M{
			"links.dataset.id": id,
		}
	} else {
		selector = bson.M{
			"links.dataset.id": id,
			"state":            publishedState,
		}
	}

	results, err := api.dataStore.Backend.GetEditions(id, selector)
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

	var selector bson.M
	if r.Header.Get(internalToken) == api.internalToken {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
		}
	} else {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"state":            publishedState,
		}
	}

	edition, err := api.dataStore.Backend.GetEdition(selector)
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

	var selector bson.M
	if r.Header.Get(internalToken) == api.internalToken {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
		}
	} else {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"state":            publishedState,
		}
	}

	results, err := api.dataStore.Backend.GetVersions(selector)
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

	var selector bson.M
	if r.Header.Get(internalToken) == api.internalToken {
		selector = bson.M{
			"links.dataset.id": id,
			"version":          version,
			"edition":          editionID,
		}
	} else {
		selector = bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"version":          version,
			"state":            publishedState,
		}
	}

	results, err := api.dataStore.Backend.GetVersion(selector)
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
	dataset.Links.Self = "/datasets/" + datasetID
	dataset.Links.Editions = "/datasets/" + datasetID + "/editions"
	dataset.LastUpdated = time.Now()

	var datasetDoc *models.DatasetUpdate
	// Do we need this functionality? Do we want the internal user to be able to publish
	// a dataset with no editions or versions. Once a version is published then an update
	// to the edition and dataset resources can occur
	if dataset.State == publishedState {
		datasetDoc = &models.DatasetUpdate{
			ID:      datasetID,
			Current: dataset,
		}
	} else {
		datasetDoc = &models.DatasetUpdate{
			ID:   datasetID,
			Next: dataset,
		}
	}

	if err := api.dataStore.Backend.UpsertDataset(datasetID, bson.M{"$set": datasetDoc}); err != nil {
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
	editionID := vars["edition"]
	if r.Header.Get(internalToken) != api.internalToken {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	edition, err := models.CreateEdition(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	edition.Edition = editionID
	edition.Links.Dataset.ID = datasetID
	edition.Links.Dataset.Link = "/datasets/" + datasetID
	edition.Links.Self = "/datasets/" + datasetID + "/editions/" + editionID
	edition.Links.Versions = "/datasets/" + datasetID + "/editions/" + editionID + "/versions"

	update := bson.M{
		"$set": edition,
		"$setOnInsert": bson.M{
			"last_updatedt": time.Now(),
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

	editionDoc, err := api.dataStore.Backend.GetEdition(bson.M{"links.dataset.id": datasetID, "edition": edition})
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
	version.Version = versionID
	version.Edition = edition
	version.Links.Dataset.ID = datasetID
	version.Links.Dataset.Link = "/datasets/" + datasetID
	version.Links.Edition.Link = "/datasets/" + datasetID + "/editions/" + edition
	version.Links.Edition.ID = editionDoc.ID
	version.Links.Self = "/datasets/" + datasetID + "/editions/" + edition + "/versions/" + versionID
	version.Links.Dimensions = "/instance/" + versionID + "/dimensions"

	update := bson.M{
		"$set": version,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	if version.State != "created" && version.CollectionID == "" {
		log.Error(errors.New("Missing association between version and a collection"), nil)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := api.dataStore.Backend.UpsertVersion(version.ID, update); err != nil {
		log.ErrorR(r, err, nil)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if version.State == publishedState {
		if err := api.updateEdition(editionDoc.ID); err != nil {
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
		if err := api.updateDatasetWithAssociation(datasetID, version); err != nil {
			log.ErrorC("failed to update dataset document after a version of a dataset has been associated with a collection", err, nil)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusCreated)

	log.Debug("upsert version", log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
}

func (api *DatasetAPI) updateEdition(id string) error {
	update := bson.M{
		"$set": bson.M{
			"state": publishedState,
		},
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	if err := api.dataStore.Backend.UpdateEdition(id, update); err != nil {
		return err
	}
	return nil
}

func (api *DatasetAPI) updateDataset(id string, version *models.Version) error {
	currentDataset, err := api.dataStore.Backend.GetDataset(id)
	if err != nil {
		log.ErrorC("Unable to update dataset", err, log.Data{"dataset_id": id})
		return err
	}

	currentDataset.Next.CollectionID = version.CollectionID
	currentDataset.Next.Links.LatestVersion.ID = version.ID
	currentDataset.Next.Links.LatestVersion.Link = version.Links.Self
	currentDataset.Next.State = publishedState
	currentDataset.Next.LastUpdated = time.Now()

	// newDataset.Next will not be cleaned up due to keeping request to mongo
	// idempotent; for instance if an authorised user double clicked to update
	// dataset, the next sub document would not exist to create the correct
	// current sub document
	newDataset := models.DatasetUpdate{
		ID:      currentDataset.ID,
		Current: currentDataset.Next,
	}

	if err := api.dataStore.Backend.UpsertDataset(id, bson.M{"$set": newDataset}); err != nil {
		log.ErrorC("Unable to update dataset", err, log.Data{"dataset_id": id})
		return err
	}

	return nil
}

func (api *DatasetAPI) updateDatasetWithAssociation(id string, version *models.Version) error {
	update := bson.M{
		"$set": bson.M{
			"next.state":                     associatedState,
			"next.collection_id":             version.CollectionID,
			"next.links.latest_version.link": version.Links.Self,
			"next.links.latest_version.id":   version.ID,
			"next.last_updated":              time.Now(),
		},
	}

	if err := api.dataStore.Backend.UpdateDataset(id, update); err != nil {
		return err
	}

	return nil
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
