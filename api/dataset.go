package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ONSdigital/dp-dataset-api/models"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
)

const (
	createdState    = "created"
	completedState  = "completed"
	associatedState = "associated"
	publishedState  = "published"

	internalToken = "Internal-Token"

	datasetDocType         = "dataset"
	editionDocType         = "edition"
	versionDocType         = "version"
	instanceDocType        = "instance"
	dimensionDocType       = "dimension"
	dimensionOptionDocType = "dimension-option"
)

func (api *DatasetAPI) getDatasets(w http.ResponseWriter, r *http.Request) {
	results, err := api.dataStore.Backend.GetDatasets()
	if err != nil {
		log.Error(err, nil)
		handleErrorType(datasetDocType, err, w)
		return
	}

	var bytes []byte

	if r.Header.Get(internalToken) == api.internalToken {
		datasets := &models.DatasetUpdateResults{}

		datasets.Items = results
		bytes, err = json.Marshal(datasets)
		if err != nil {
			log.Error(err, nil)
			handleErrorType(datasetDocType, err, w)
			return
		}
	} else {
		datasets := &models.DatasetResults{}

		datasets.Items = mapResults(results)

		bytes, err = json.Marshal(datasets)
		if err != nil {
			log.Error(err, nil)
			handleErrorType(datasetDocType, err, w)
			return
		}
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
		handleErrorType(datasetDocType, err, w)
		return
	}

	var bytes []byte
	if r.Header.Get(internalToken) != api.internalToken {
		if dataset.Current == nil {
			handleErrorType(datasetDocType, errs.DatasetNotFound, w)
			return
		}

		dataset.Current.ID = dataset.ID
		bytes, err = json.Marshal(dataset.Current)
		if err != nil {
			log.Error(err, log.Data{"dataset_id": id})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		if dataset == nil {
			handleErrorType(datasetDocType, errs.DatasetNotFound, w)
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

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.Error(err, log.Data{"dataset_id": id})
		handleErrorType(editionDocType, err, w)
		return
	}

	results, err := api.dataStore.Backend.GetEditions(id, state)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id})
		handleErrorType(editionDocType, err, w)
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

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.Error(err, log.Data{"dataset_id": id})
		handleErrorType(editionDocType, err, w)
		return
	}

	edition, err := api.dataStore.Backend.GetEdition(id, editionID, state)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID})
		handleErrorType(editionDocType, err, w)
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

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID})
		handleErrorType(versionDocType, err, w)
		return
	}

	if err := api.dataStore.Backend.CheckEditionExists(id, editionID, state); err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID})
		handleErrorType(versionDocType, err, w)
		return
	}

	results, err := api.dataStore.Backend.GetVersions(id, editionID, state)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID})
		handleErrorType(versionDocType, err, w)
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

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	if err := api.dataStore.Backend.CheckEditionExists(id, editionID, state); err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	results, err := api.dataStore.Backend.GetVersion(id, editionID, version, state)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	results.Links.Self.HRef = results.Links.Version.HRef

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
	vars := mux.Vars(r)
	datasetID := vars["id"]

	_, err := api.dataStore.Backend.GetDataset(datasetID)
	if err != nil {
		if err != errs.DatasetNotFound {
			log.Error(err, log.Data{"dataset_id": datasetID})
			handleErrorType(datasetDocType, err, w)
			return
		}
	} else {
		err := fmt.Errorf("Forbidden - dataset already exists")
		log.Error(err, log.Data{"dataset_id": datasetID})
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	dataset, err := models.CreateDataset(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var accessRights string
	if dataset.Links != nil {
		if dataset.Links.AccessRights != nil {
			if dataset.Links.AccessRights.HRef != "" {
				accessRights = dataset.Links.AccessRights.HRef
			}
		}
	}

	dataset.ID = datasetID
	dataset.Links = &models.DatasetLinks{
		Editions: &models.LinkObject{
			HRef: fmt.Sprintf("%s/datasets/%s/editions", api.host, datasetID),
		},
		Self: &models.LinkObject{
			HRef: fmt.Sprintf("%s/datasets/%s", api.host, datasetID),
		},
	}

	if accessRights != "" {
		dataset.Links.AccessRights = &models.LinkObject{
			HRef: accessRights,
		}
	}

	dataset.LastUpdated = time.Now()

	datasetDoc := &models.DatasetUpdate{
		ID:   datasetID,
		Next: dataset,
	}

	if err := api.dataStore.Backend.UpsertDataset(datasetID, datasetDoc); err != nil {
		log.ErrorR(r, err, nil)
		handleErrorType(datasetDocType, err, w)
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

func (api *DatasetAPI) putDataset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]

	dataset, err := models.CreateDataset(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := api.dataStore.Backend.UpdateDataset(datasetID, dataset); err != nil {
		handleErrorType(datasetDocType, err, w)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.Debug("update dataset", log.Data{"dataset_id": datasetID})
}

func (api *DatasetAPI) putVersion(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	editionID := vars["edition"]
	versionID := vars["version"]

	version, err := models.CreateVersion(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	currentVersion, err := api.dataStore.Backend.GetVersion(datasetID, editionID, versionID, "")
	if err != nil {
		handleErrorType(versionDocType, err, w)
		return
	}

	// Check current state of version document;
	// if published do not try to update document
	if currentVersion.State == publishedState {
		http.Error(w, fmt.Sprintf("Unable to update document, already published"), http.StatusForbidden)
		return
	}

	// Combine update version document to existing version document
	newVersion := createNewVersionDoc(currentVersion, version)

	if err = models.ValidateVersion(newVersion); err != nil {
		log.ErrorR(r, err, nil)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := api.dataStore.Backend.UpdateVersion(newVersion.ID, version); err != nil {
		handleErrorType(versionDocType, err, w)
		return
	}

	if version.State == publishedState {
		if err := api.dataStore.Backend.UpdateEdition(datasetID, editionID, version.State); err != nil {
			log.ErrorC("failed to update the state of edition document to published", err, nil)
			handleErrorType(versionDocType, err, w)
			return
		}

		// Pass in newVersion variable to include relevant data needed for update on dataset API (e.g. links)
		if err := api.updateDataset(datasetID, newVersion); err != nil {
			log.ErrorC("failed to update dataset document once version state changes to publish", err, nil)
			handleErrorType(versionDocType, err, w)
			return
		}
	}

	if version.State == associatedState {
		if err := api.dataStore.Backend.UpdateDatasetWithAssociation(datasetID, associatedState, version); err != nil {
			log.ErrorC("failed to update dataset document after a version of a dataset has been associated with a collection", err, nil)
			handleErrorType(versionDocType, err, w)
			return
		}
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.Debug("update dataset", log.Data{"dataset_id": datasetID})
}

func createNewVersionDoc(currentVersion *models.Version, version *models.Version) *models.Version {
	if version.CollectionID == "" {
		version.CollectionID = currentVersion.CollectionID
	}

	if version.ReleaseDate == "" {
		version.ReleaseDate = currentVersion.ReleaseDate
	}

	if version.State == "" {
		version.State = currentVersion.State
	}

	if version.Temporal == nil {
		version.Temporal = currentVersion.Temporal
	}

	var spatial string

	// Get spatial link before overwriting the version links object below
	if version.Links != nil {
		if version.Links.Spatial != nil {
			if version.Links.Spatial.HRef != "" {
				spatial = version.Links.Spatial.HRef
			}
		}
	}

	version.ID = currentVersion.ID
	version.Links = currentVersion.Links

	if spatial != "" {

		// In reality the current version will always have a link object, so
		// if/else statement should always fall into else block
		if version.Links == nil {
			version.Links = &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: spatial,
				},
			}
		} else {
			version.Links.Spatial = &models.LinkObject{
				HRef: spatial,
			}
		}
	}

	return version
}

func (api *DatasetAPI) updateDataset(id string, version *models.Version) error {
	currentDataset, err := api.dataStore.Backend.GetDataset(id)
	if err != nil {
		log.ErrorC("Unable to update dataset", err, log.Data{"dataset_id": id})
		return err
	}

	currentDataset.Next.CollectionID = version.CollectionID
	currentDataset.Next.Links = &models.DatasetLinks{
		Editions: &models.LinkObject{
			HRef: fmt.Sprintf("%s/datasets/%s/editions", api.host, version.Links.Dataset.ID),
		},
		LatestVersion: &models.LinkObject{
			ID:   version.ID,
			HRef: version.Links.Self.HRef,
		},
		Self: &models.LinkObject{
			HRef: fmt.Sprintf("%s/datasets/%s", api.host, version.Links.Dataset.ID),
		},
	}
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

func (api *DatasetAPI) getDimensions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	editionID := vars["edition"]
	versionID := vars["version"]

	results, err := api.dataStore.Backend.GetDimensions(datasetID, editionID, versionID)
	if err != nil {
		handleErrorType(dimensionDocType, err, w)
		return
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": datasetID, "edition": editionID, "version": versionID})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": datasetID, "edition": editionID, "version": versionID})
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Debug("get dimensions", log.Data{"dataset_id": datasetID, "edition": editionID, "version": versionID})
}

func (api *DatasetAPI) getDimensionOptions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	editionID := vars["edition"]
	versionID := vars["version"]
	dimension := vars["dimension"]
	results, err := api.dataStore.Backend.GetDimensionOptions(datasetID, editionID, versionID, dimension)
	if err != nil {
		handleErrorType(dimensionOptionDocType, err, w)
		return
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": datasetID, "edition": editionID, "version": versionID, "dimension": dimension})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": datasetID, "edition": editionID, "version": versionID, "dimension": dimension})
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Debug("get dimension options", log.Data{"dataset_id": datasetID, "edition": editionID, "version": versionID, "dimension": dimension})
}

func mapResults(results []models.DatasetUpdate) []*models.Dataset {
	items := []*models.Dataset{}
	for _, item := range results {
		if item.Current == nil {
			continue
		}
		item.Current.ID = item.ID

		items = append(items, item.Current)
	}
	return items
}

func handleErrorType(docType string, err error, w http.ResponseWriter) {
	log.Error(err, nil)

	switch docType {
	default:
		if err == errs.DatasetNotFound || err == errs.EditionNotFound || err == errs.VersionNotFound || err == errs.DimensionNodeNotFound || err == errs.InstanceNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	case "edition":
		if err == errs.DatasetNotFound {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else if err == errs.EditionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	case "version":
		if err == errs.DatasetNotFound {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else if err == errs.EditionNotFound {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else if err == errs.VersionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func setJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}
