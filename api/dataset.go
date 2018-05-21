package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
)

func (api *DatasetAPI) getDatasets(w http.ResponseWriter, r *http.Request) {
	if err := api.auditor.Record(r.Context(), getDatasetsAction, actionAttempted, nil); err != nil {
		handleAuditingFailure(w, err, nil)
		return
	}

	results, err := api.dataStore.Backend.GetDatasets()
	if err != nil {
		log.Error(err, nil)
		if auditErr := api.auditor.Record(r.Context(), getDatasetsAction, actionUnsuccessful, nil); auditErr != nil {
			handleAuditingFailure(w, auditErr, nil)
			return
		}
		handleErrorType(datasetDocType, err, w)
		return
	}

	authorised, logData := api.authenticate(r, log.Data{})

	var b []byte
	if authorised {

		// User has valid authentication to get raw dataset document
		datasets := &models.DatasetUpdateResults{}
		datasets.Items = results
		b, err = json.Marshal(datasets)
		if err != nil {
			log.ErrorC("failed to marshal dataset resource into bytes", err, nil)
			handleErrorType(datasetDocType, err, w)
			return
		}
	} else {

		// User is not authenticated and hence has only access to current sub document
		datasets := &models.DatasetResults{}
		datasets.Items = mapResults(results)

		b, err = json.Marshal(datasets)
		if err != nil {
			log.ErrorC("failed to marshal dataset resource into bytes", err, nil)
			handleErrorType(datasetDocType, err, w)
			return
		}
	}

	if auditErr := api.auditor.Record(r.Context(), getDatasetsAction, actionSuccessful, nil); auditErr != nil {
		handleAuditingFailure(w, auditErr, logData)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug("get all datasets", logData)
}

func (api *DatasetAPI) getDataset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	logData := log.Data{"dataset_id": id}
	auditParams := common.Params{"dataset_id": id}

	if auditErr := api.auditor.Record(r.Context(), getDatasetAction, actionAttempted, auditParams); auditErr != nil {
		handleAuditingFailure(w, auditErr, logData)
		return
	}

	dataset, err := api.dataStore.Backend.GetDataset(id)
	if err != nil {
		log.Error(err, logData)
		if auditErr := api.auditor.Record(r.Context(), getDatasetAction, actionUnsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleErrorType(datasetDocType, err, w)
		return
	}

	authorised, logData := api.authenticate(r, logData)

	var b []byte
	if !authorised {
		// User is not authenticated and hence has only access to current sub document
		if dataset.Current == nil {
			log.Debug("published dataset not found", nil)
			handleErrorType(datasetDocType, errs.ErrDatasetNotFound, w)
			return
		}

		dataset.Current.ID = dataset.ID
		b, err = json.Marshal(dataset.Current)
		if err != nil {
			log.ErrorC("failed to marshal dataset current sub document resource into bytes", err, logData)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		// User has valid authentication to get raw dataset document
		if dataset == nil {
			log.Debug("published or unpublished dataset not found", logData)
			handleErrorType(datasetDocType, errs.ErrDatasetNotFound, w)
		}
		b, err = json.Marshal(dataset)
		if err != nil {
			log.ErrorC("failed to marshal dataset current sub document resource into bytes", err, logData)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if auditErr := api.auditor.Record(r.Context(), getDatasetAction, actionSuccessful, auditParams); auditErr != nil {
		handleAuditingFailure(w, auditErr, logData)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug("get dataset", logData)
}

func (api *DatasetAPI) addDataset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]

	// TODO Could just do an insert, if dataset already existed we would get a duplicate key error
	// instead of reading then writing doc
	_, err := api.dataStore.Backend.GetDataset(datasetID)
	if err != nil {
		if err != errs.ErrDatasetNotFound {
			log.ErrorC("failed to check if dataset exists", err, log.Data{"dataset_id": datasetID})
			handleErrorType(datasetDocType, err, w)
			return
		}
	} else {
		err = fmt.Errorf("forbidden - dataset already exists")
		log.ErrorC("unable to create a dataset that already exists", err, log.Data{"dataset_id": datasetID})
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	dataset, err := models.CreateDataset(r.Body)
	if err != nil {
		log.ErrorC("failed to model dataset resource based on request", err, log.Data{"dataset_id": datasetID})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	dataset.State = models.CreatedState
	dataset.ID = datasetID

	if dataset.Links == nil {
		dataset.Links = &models.DatasetLinks{}
	}

	dataset.Links.Editions = &models.LinkObject{
		HRef: fmt.Sprintf("%s/datasets/%s/editions", api.host, datasetID),
	}

	dataset.Links.Self = &models.LinkObject{
		HRef: fmt.Sprintf("%s/datasets/%s", api.host, datasetID),
	}

	dataset.LastUpdated = time.Now()

	datasetDoc := &models.DatasetUpdate{
		ID:   datasetID,
		Next: dataset,
	}

	if err = api.dataStore.Backend.UpsertDataset(datasetID, datasetDoc); err != nil {
		log.ErrorC("failed to insert dataset resource to datastore", err, log.Data{"new_dataset": datasetID})
		handleErrorType(datasetDocType, err, w)
		return
	}

	b, err := json.Marshal(datasetDoc)
	if err != nil {
		log.ErrorC("failed to marshal dataset resource into bytes", err, log.Data{"new_dataset": datasetID})
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusCreated)
	_, err = w.Write(b)
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
		log.ErrorC("failed to model dataset resource based on request", err, log.Data{"dataset_id": datasetID})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	currentDataset, err := api.dataStore.Backend.GetDataset(datasetID)
	if err != nil {
		log.ErrorC("failed to find dataset", err, log.Data{"dataset_id": datasetID})
		handleErrorType(datasetDocType, err, w)
		return
	}

	if dataset.State == models.PublishedState {
		if err := api.publishDataset(currentDataset, nil); err != nil {
			log.ErrorC("failed to update dataset document to published", err, log.Data{"dataset_id": datasetID})
			handleErrorType(versionDocType, err, w)
			return
		}
	} else {
		if err := api.dataStore.Backend.UpdateDataset(datasetID, dataset, currentDataset.Next.State); err != nil {
			log.ErrorC("failed to update dataset resource", err, log.Data{"dataset_id": datasetID})
			handleErrorType(datasetDocType, err, w)
			return
		}
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.Debug("update dataset", log.Data{"dataset_id": datasetID})
}

func (api *DatasetAPI) publishDataset(currentDataset *models.DatasetUpdate, version *models.Version) error {
	if version != nil {
		currentDataset.Next.CollectionID = ""

		currentDataset.Next.Links.LatestVersion = &models.LinkObject{
			ID:   version.Links.Version.ID,
			HRef: version.Links.Version.HRef,
		}
	}

	currentDataset.Next.State = models.PublishedState
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

	if err := api.dataStore.Backend.UpsertDataset(currentDataset.ID, newDataset); err != nil {
		log.ErrorC("unable to update dataset", err, log.Data{"dataset_id": currentDataset.ID})
		return err
	}

	return nil
}

func (api *DatasetAPI) deleteDataset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]

	currentDataset, err := api.dataStore.Backend.GetDataset(datasetID)
	if err == errs.ErrDatasetNotFound {
		log.Debug("cannot delete dataset, it does not exist", log.Data{"dataset_id": datasetID})
		w.WriteHeader(http.StatusNoContent) // idempotent
		return
	}
	if err != nil {
		log.ErrorC("failed to run query for existing dataset", err, log.Data{"dataset_id": datasetID})
		handleErrorType(datasetDocType, err, w)
		return
	}

	if currentDataset.Current != nil && currentDataset.Current.State == models.PublishedState {
		err = fmt.Errorf("forbidden - a published dataset cannot be deleted")
		log.ErrorC("unable to delete a published dataset", err, log.Data{"dataset_id": datasetID})
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	if err := api.dataStore.Backend.DeleteDataset(datasetID); err != nil {
		log.ErrorC("failed to delete dataset", err, log.Data{"dataset_id": datasetID})
		handleErrorType(datasetDocType, err, w)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	log.Debug("delete dataset", log.Data{"dataset_id": datasetID})
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
