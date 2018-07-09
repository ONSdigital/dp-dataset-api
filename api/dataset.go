package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

var (
	// errors that should return a 403 status
	datasetsForbidden = map[error]bool{
		errs.ErrDeletePublishedDatasetForbidden: true,
		errs.ErrAddDatasetAlreadyExists:         true,
	}

	// errors that should return a 404 status
	datasetsNotFound = map[error]bool{
		errs.ErrDatasetNotFound: true,
	}

	// errors that should return a 204 status
	datasetsNoContent = map[error]bool{
		errs.ErrDeleteDatasetNotFound: true,
	}

	// errors that should return a 400 status
	datasetsBadRequest = map[error]bool{
		errs.ErrAddUpdateDatasetBadRequest: true,
	}
)

func (api *DatasetAPI) getDatasets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := api.auditor.Record(ctx, getDatasetsAction, audit.Attempted, nil); err != nil {
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		return
	}

	b, err := func() ([]byte, error) {
		datasets, err := api.dataStore.Backend.GetDatasets()
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "api endpoint getDatasets datastore.GetDatasets returned an error"), nil)
			return nil, err
		}
		authorised, logData := api.authenticate(r, log.Data{})

		var b []byte
		var datasetsResponse interface{}

		if authorised {
			// User has valid authentication to get raw dataset document
			datasetsResponse = &models.DatasetUpdateResults{Items: datasets}
		} else {
			// User is not authenticated and hence has only access to current sub document
			datasetsResponse = &models.DatasetResults{Items: mapResults(datasets)}
		}

		b, err = json.Marshal(datasetsResponse)

		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "api endpoint getDatasets failed to marshal dataset resource into bytes"), logData)
			return nil, err
		}

		return b, nil
	}()

	if err != nil {
		if auditErr := api.auditor.Record(ctx, getDatasetsAction, audit.Unsuccessful, nil); auditErr != nil {
			err = auditErr
		}
		handleDatasetAPIErr(ctx, err, w, nil)
		return
	}

	if auditErr := api.auditor.Record(ctx, getDatasetsAction, audit.Successful, nil); auditErr != nil {
		handleDatasetAPIErr(ctx, auditErr, w, nil)
		return
	}

	setJSONContentType(w)
	if _, err = w.Write(b); err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "api endpoint getDatasets error writing response body"), nil)
		handleDatasetAPIErr(ctx, err, w, nil)
		return
	}
	log.InfoCtx(ctx, "api endpoint getDatasets request successful", nil)
}

func (api *DatasetAPI) getDataset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	id := vars["id"]
	logData := log.Data{"dataset_id": id}
	auditParams := common.Params{"dataset_id": id}

	if auditErr := api.auditor.Record(ctx, getDatasetAction, audit.Attempted, auditParams); auditErr != nil {
		handleDatasetAPIErr(ctx, errs.ErrInternalServer, w, logData)
		return
	}

	b, err := func() ([]byte, error) {
		dataset, err := api.dataStore.Backend.GetDataset(id)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "getDataset endpoint: dataStore.Backend.GetDataset returned an error"), logData)
			return nil, err
		}

		authorised, logData := api.authenticate(r, logData)

		var b []byte
		var datasetResponse interface{}

		//		var marshallErr error
		if !authorised {
			// User is not authenticated and hence has only access to current sub document
			if dataset.Current == nil {
				log.InfoCtx(ctx, "getDataste endpoint: published dataset not found", logData)
				return nil, errs.ErrDatasetNotFound
			}

			log.InfoCtx(ctx, "getDataset endpoint: caller authorised returning dataset current sub document", logData)

			dataset.Current.ID = dataset.ID
			datasetResponse = dataset.Current
		} else {
			// User has valid authentication to get raw dataset document
			if dataset == nil {
				log.InfoCtx(ctx, "getDataset endpoint: published or unpublished dataset not found", logData)
				return nil, errs.ErrDatasetNotFound
			}
			log.InfoCtx(ctx, "getDataset endpoint: caller not authorised returning dataset", logData)
			datasetResponse = dataset
		}

		b, err = json.Marshal(datasetResponse)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "getDataset endpoint: failed to marshal dataset resource into bytes"), logData)
			return nil, err
		}

		return b, nil
	}()

	if err != nil {
		if auditErr := api.auditor.Record(ctx, getDatasetAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	if auditErr := api.auditor.Record(ctx, getDatasetAction, audit.Successful, auditParams); auditErr != nil {
		handleDatasetAPIErr(ctx, auditErr, w, logData)
		return
	}

	setJSONContentType(w)
	if _, err = w.Write(b); err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "getDataset endpoint: error writing bytes to response"), logData)
		handleDatasetAPIErr(ctx, err, w, logData)
	}
	log.InfoCtx(ctx, "getDataset endpoint: request successful", logData)
}

func (api *DatasetAPI) addDataset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["id"]

	logData := log.Data{"dataset_id": datasetID}
	auditParams := common.Params{"dataset_id": datasetID}

	if auditErr := api.auditor.Record(ctx, addDatasetAction, audit.Attempted, auditParams); auditErr != nil {
		handleDatasetAPIErr(ctx, auditErr, w, logData)
		return
	}

	// TODO Could just do an insert, if dataset already existed we would get a duplicate key error instead of reading then writing doc
	b, err := func() ([]byte, error) {
		_, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			if err != errs.ErrDatasetNotFound {
				log.ErrorCtx(ctx, errors.WithMessage(err, "addDataset endpoint: error checking if dataset exists"), logData)
				return nil, err
			}
		} else {
			log.ErrorCtx(ctx, errors.WithMessage(errs.ErrAddDatasetAlreadyExists, "addDataset endpoint: unable to create a dataset that already exists"), logData)
			return nil, errs.ErrAddDatasetAlreadyExists
		}

		defer r.Body.Close()
		dataset, err := models.CreateDataset(r.Body)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "addDataset endpoint: failed to model dataset resource based on request"), logData)
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
			log.ErrorCtx(ctx, errors.WithMessage(err, "addDataset endpoint: failed to insert dataset resource to datastore"), logData)
			return nil, err
		}

		b, err := json.Marshal(datasetDoc)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "addDataset endpoint: failed to marshal dataset resource into bytes"), logData)
			return nil, err
		}
		return b, nil
	}()

	if err != nil {
		api.auditor.Record(ctx, addDatasetAction, audit.Unsuccessful, auditParams)
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	api.auditor.Record(ctx, addDatasetAction, audit.Successful, auditParams)

	setJSONContentType(w)
	w.WriteHeader(http.StatusCreated)
	if _, err = w.Write(b); err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "addDataset endpoint: error writing bytes to response"), logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.InfoCtx(ctx, "addDataset endpoint: request completed successfully", logData)
}

func (api *DatasetAPI) putDataset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["id"]
	data := log.Data{"dataset_id": datasetID}
	auditParams := common.Params{"dataset_id": datasetID}

	if auditErr := api.auditor.Record(ctx, putDatasetAction, audit.Attempted, auditParams); auditErr != nil {
		handleDatasetAPIErr(ctx, auditErr, w, data)
		return
	}

	err := func() error {
		defer r.Body.Close()

		dataset, err := models.CreateDataset(r.Body)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "putDataset endpoint: failed to model dataset resource based on request"), data)
			return errs.ErrAddUpdateDatasetBadRequest
		}

		currentDataset, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "putDataset endpoint: datastore.getDataset returned an error"), data)
			return err
		}

		if dataset.State == models.PublishedState {
			if err := api.publishDataset(ctx, currentDataset, nil); err != nil {
				log.ErrorCtx(ctx, errors.WithMessage(err, "putDataset endpoint: failed to update dataset document to published"), data)
				return err
			}
		} else {
			if err := api.dataStore.Backend.UpdateDataset(datasetID, dataset, currentDataset.Next.State); err != nil {
				log.ErrorCtx(ctx, errors.WithMessage(err, "putDataset endpoint: failed to update dataset resource"), data)
				return err
			}
		}
		return nil
	}()

	if err != nil {
		api.auditor.Record(ctx, putDatasetAction, audit.Unsuccessful, auditParams)
		handleDatasetAPIErr(ctx, err, w, data)
		return
	}

	api.auditor.Record(ctx, putDatasetAction, audit.Successful, auditParams)

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.InfoCtx(ctx, "putDataset endpoint: request successful", data)
}

func (api *DatasetAPI) publishDataset(ctx context.Context, currentDataset *models.DatasetUpdate, version *models.Version) error {
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
		log.ErrorCtx(ctx, errors.WithMessage(err, "unable to update dataset"), log.Data{"dataset_id": currentDataset.ID})
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

	if auditErr := api.auditor.Record(ctx, deleteDatasetAction, audit.Attempted, auditParams); auditErr != nil {
		handleDatasetAPIErr(ctx, auditErr, w, logData)
		return
	}

	// attempt to delete the dataset.
	err := func() error {
		currentDataset, err := api.dataStore.Backend.GetDataset(datasetID)

		if err == errs.ErrDatasetNotFound {
			log.InfoCtx(ctx, "cannot delete dataset, it does not exist", logData)
			return errs.ErrDeleteDatasetNotFound
		}

		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "failed to run query for existing dataset"), logData)
			return err
		}

		if currentDataset.Current != nil && currentDataset.Current.State == models.PublishedState {
			log.ErrorCtx(ctx, errors.WithMessage(errs.ErrDeletePublishedDatasetForbidden, "unable to delete a published dataset"), logData)
			return errs.ErrDeletePublishedDatasetForbidden
		}

		if err := api.dataStore.Backend.DeleteDataset(datasetID); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "failed to delete dataset"), logData)
			return err
		}
		log.InfoCtx(ctx, "dataset deleted successfully", logData)
		return nil
	}()

	if err != nil {
		api.auditor.Record(ctx, deleteDatasetAction, audit.Unsuccessful, auditParams)
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	api.auditor.Record(ctx, deleteDatasetAction, audit.Successful, auditParams)
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

func handleDatasetAPIErr(ctx context.Context, err error, w http.ResponseWriter, data log.Data) {
	if data == nil {
		data = log.Data{}
	}

	var status int
	switch {
	case datasetsForbidden[err]:
		status = http.StatusForbidden
	case datasetsNotFound[err]:
		status = http.StatusNotFound
	case datasetsNoContent[err]:
		status = http.StatusNoContent
	case datasetsBadRequest[err]:
		status = http.StatusBadRequest
	default:
		err = errs.ErrInternalServer
		status = http.StatusInternalServerError
	}

	data["responseStatus"] = status
	log.ErrorCtx(ctx, errors.WithMessage(err, "request unsuccessful"), data)
	http.Error(w, err.Error(), status)
}
