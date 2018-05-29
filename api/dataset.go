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
	"github.com/pkg/errors"
)

func (api *DatasetAPI) getDatasets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := api.auditor.Record(ctx, getDatasetsAction, actionAttempted, nil); err != nil {
		auditActionFailure(ctx, getDatasetsAction, actionAttempted, err, nil)
		handleErrorType(datasetDocType, errs.ErrAuditActionAttemptedFailure, w)
		return
	}

	b, err := func() ([]byte, error) {
		results, err := api.dataStore.Backend.GetDatasets()
		if err != nil {
			logError(ctx, errors.WithMessage(err, "api endpoint getDatasets datastore.GetDatasets returned an error"), nil)
			return nil, err
		}
		authorised, logData := api.authenticate(r, log.Data{})

		var b []byte
		if authorised {

			// User has valid authentication to get raw dataset document
			datasets := &models.DatasetUpdateResults{}
			datasets.Items = results
			b, err = json.Marshal(datasets)
			if err != nil {
				logError(ctx, errors.WithMessage(err, "api endpoint getDatasets failed to marshal dataset resource into bytes"), logData)
				return nil, err
			}
		} else {

			// User is not authenticated and hence has only access to current sub document
			datasets := &models.DatasetResults{}
			datasets.Items = mapResults(results)

			b, err = json.Marshal(datasets)
			if err != nil {
				logError(ctx, errors.WithMessage(err, "api endpoint getDatasets failed to marshal dataset resource into bytes"), logData)
				return nil, err
			}
		}
		return b, err
	}()

	if err != nil {
		if auditErr := api.auditor.Record(ctx, getDatasetsAction, actionUnsuccessful, nil); auditErr != nil {
			auditActionFailure(ctx, getDatasetsAction, actionUnsuccessful, auditErr, nil)
		}
		handleErrorType(datasetDocType, err, w)
		return
	}

	if auditErr := api.auditor.Record(ctx, getDatasetsAction, actionSuccessful, nil); auditErr != nil {
		auditActionFailure(ctx, getDatasetsAction, actionSuccessful, auditErr, nil)
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		logError(ctx, errors.WithMessage(err, "api endpoint getDatasets error writing response body"), nil)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	logInfo(ctx, "api endpoint getDatasets request successful", nil)
}

func (api *DatasetAPI) getDataset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	id := vars["id"]
	logData := log.Data{"dataset_id": id}
	auditParams := common.Params{"dataset_id": id}

	if auditErr := api.auditor.Record(ctx, getDatasetAction, actionAttempted, auditParams); auditErr != nil {
		auditActionFailure(ctx, getDatasetAction, actionAttempted, auditErr, logData)
		handleErrorType(datasetDocType, errs.ErrInternalServer, w)
		return
	}

	b, err := func() ([]byte, error) {
		dataset, err := api.dataStore.Backend.GetDataset(id)
		if err != nil {
			logError(ctx, errors.WithMessage(err, "getDataset endpoint: dataStore.Backend.GetDataset returned an error"), logData)
			return nil, err
		}

		authorised, logData := api.authenticate(r, logData)

		var b []byte
		if !authorised {
			// User is not authenticated and hence has only access to current sub document
			if dataset.Current == nil {
				logInfo(ctx, "getDataste endpoint: published dataset not found", logData)
				return nil, errs.ErrDatasetNotFound
			}

			dataset.Current.ID = dataset.ID
			b, err = json.Marshal(dataset.Current)
			if err != nil {
				logError(ctx, errors.WithMessage(err, "getDataset endpoint: failed to marshal dataset current sub document resource into bytes"), logData)
				return nil, err
			}
		} else {
			// User has valid authentication to get raw dataset document
			if dataset == nil {
				logInfo(ctx, "getDataset endpoint: published or unpublished dataset not found", logData)
				return nil, errs.ErrDatasetNotFound
			}
			b, err = json.Marshal(dataset)
			if err != nil {
				logError(ctx, errors.WithMessage(err, "getDataset endpoint: failed to marshal dataset current sub document resource into bytes"), logData)
				return nil, err
			}
		}
		return b, nil
	}()

	if err != nil {
		if auditErr := api.auditor.Record(ctx, getDatasetAction, actionUnsuccessful, auditParams); auditErr != nil {
			auditActionFailure(ctx, getDatasetAction, actionUnsuccessful, auditErr, logData)
		}
		handleErrorType(datasetDocType, err, w)
		return
	}

	if auditErr := api.auditor.Record(ctx, getDatasetAction, actionSuccessful, auditParams); auditErr != nil {
		auditActionFailure(ctx, getDatasetAction, actionSuccessful, auditErr, logData)
		handleErrorType(datasetDocType, errs.ErrInternalServer, w)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		logError(ctx, errors.WithMessage(err, "getDataset endpoint: error writing bytes to response"), logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	logInfo(ctx, "getDataset endpoint: request successful", logData)
}

func (api *DatasetAPI) addDataset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["id"]

	logData := log.Data{"dataset_id": datasetID}
	auditParams := common.Params{"dataset_id": datasetID}

	if err := api.auditor.Record(ctx, addDatasetAction, actionAttempted, auditParams); err != nil {
		auditActionFailure(ctx, addDatasetAction, actionAttempted, err, logData)
		handleErrorType(datasetDocType, errs.ErrInternalServer, w)
		return
	}

	// TODO Could just do an insert, if dataset already existed we would get a duplicate key error instead of reading then writing doc
	b, err := func() ([]byte, error) {
		_, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			if err != errs.ErrDatasetNotFound {
				logError(ctx, errors.WithMessage(err, "addDataset endpoint: error checking if dataset exists"), logData)
				return nil, err
			}
		} else {
			logError(ctx, errors.WithMessage(errs.ErrAddDatasetAlreadyExists, "addDataset endpoint: unable to create a dataset that already exists"), logData)
			return nil, errs.ErrAddDatasetAlreadyExists
		}

		defer r.Body.Close()
		dataset, err := models.CreateDataset(r.Body)
		if err != nil {
			logError(ctx, errors.WithMessage(err, "addDataset endpoint: failed to model dataset resource based on request"), logData)
			return nil, errs.ErrAddUpdateDatasetBadRequest
		}

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
			logData["new_dataset"] = datasetID
			logError(ctx, errors.WithMessage(err, "addDataset endpoint: failed to insert dataset resource to datastore"), logData)
			return nil, err
		}

		b, err := json.Marshal(datasetDoc)
		if err != nil {
			logError(ctx, errors.WithMessage(err, "addDataset endpoint: failed to marshal dataset resource into bytes"), logData)
			return nil, err
		}
		return b, nil
	}()

	if err != nil {
		if auditErr := api.auditor.Record(ctx, addDatasetAction, actionUnsuccessful, auditParams); auditErr != nil {
			auditActionFailure(ctx, addDatasetAction, actionUnsuccessful, auditErr, logData)
		}
		handleErrorType(datasetDocType, err, w)
		return
	}

	if auditErr := api.auditor.Record(ctx, addDatasetAction, actionSuccessful, auditParams); auditErr != nil {
		auditActionFailure(ctx, addDatasetAction, actionUnsuccessful, auditErr, logData)
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusCreated)
	_, err = w.Write(b)
	if err != nil {
		logError(ctx, errors.WithMessage(err, "addDataset endpoint: error writing bytes to response"), logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	logInfo(ctx, "addDataset endpoint: request completed successfully", logData)
}

func (api *DatasetAPI) putDataset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["id"]
	data := log.Data{"dataset_id": datasetID}

	err := func() error {
		defer r.Body.Close()

		dataset, err := models.CreateDataset(r.Body)
		if err != nil {
			logError(ctx, errors.WithMessage(err, "putDataset endpoint: failed to model dataset resource based on request"), data)
			return errs.ErrAddUpdateDatasetBadRequest
		}

		currentDataset, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			logError(ctx, errors.WithMessage(err, "putDataset endpoint: datastore.getDataset returned an error"), data)
			return err
		}

		if dataset.State == models.PublishedState {
			if err := api.publishDataset(currentDataset, nil); err != nil {
				logError(ctx, errors.WithMessage(err, "putDataset endpoint: failed to update dataset document to published"), data)
				return err
			}
		} else {
			if err := api.dataStore.Backend.UpdateDataset(datasetID, dataset, currentDataset.Next.State); err != nil {
				logError(ctx, errors.WithMessage(err, "putDataset endpoint: failed to update dataset resource"), data)
				return err
			}
		}
		return nil
	}()

	if err != nil {
		handleErrorType(datasetDocType, err, w)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	logInfo(ctx, "putDataset endpoint: request successful", data)
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
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["id"]
	logData := log.Data{"dataset_id": datasetID}
	auditParams := common.Params{"dataset_id": datasetID}

	if err := api.auditor.Record(ctx, deleteDatasetAction, actionAttempted, auditParams); err != nil {
		auditActionFailure(ctx, deleteDatasetAction, actionAttempted, err, logData)
		handleErrorType(datasetDocType, err, w)
		return
	}

	// attempt to delete the dataset.
	err := func() error {
		currentDataset, err := api.dataStore.Backend.GetDataset(datasetID)

		if err == errs.ErrDatasetNotFound {
			log.Debug("cannot delete dataset, it does not exist", logData)
			return errs.ErrDeleteDatasetNotFound
		}

		if err != nil {
			log.ErrorC("failed to run query for existing dataset", err, logData)
			return err
		}

		if currentDataset.Current != nil && currentDataset.Current.State == models.PublishedState {
			log.ErrorC("unable to delete a published dataset", errs.ErrDeletePublishedDatasetForbidden, logData)
			return errs.ErrDeletePublishedDatasetForbidden
		}

		if err := api.dataStore.Backend.DeleteDataset(datasetID); err != nil {
			log.ErrorC("failed to delete dataset", err, logData)
			return err
		}
		log.Debug("dataset deleted successfully", logData)
		return nil
	}()

	if err != nil {
		if auditErr := api.auditor.Record(ctx, deleteDatasetAction, actionUnsuccessful, auditParams); auditErr != nil {
			auditActionFailure(ctx, deleteDatasetAction, actionUnsuccessful, auditErr, logData)
		}
		handleErrorType(datasetDocType, err, w)
		return
	}

	if err := api.auditor.Record(ctx, deleteDatasetAction, actionSuccessful, auditParams); err != nil {
		auditActionFailure(ctx, deleteDatasetAction, actionSuccessful, err, logData)
		// fall through and return the origin status code as the action has been carried out at this point.
	}
	w.WriteHeader(http.StatusNoContent)
	log.Debug("delete dataset", logData)
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
