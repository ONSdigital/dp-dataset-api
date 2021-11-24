package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
)

var (
	// errors that should return a 403 status
	datasetsForbidden = map[error]bool{
		errs.ErrDeletePublishedDatasetForbidden: true,
		errs.ErrAddDatasetAlreadyExists:         true,
	}

	// errors that should return a 204 status
	datasetsNoContent = map[error]bool{
		errs.ErrDeleteDatasetNotFound: true,
	}

	// errors that should return a 400 status
	datasetsBadRequest = map[error]bool{
		errs.ErrAddUpdateDatasetBadRequest: true,
		errs.ErrTypeMismatch:               true,
		errs.ErrDatasetTypeInvalid:         true,
		errs.ErrInvalidQueryParameter:      true,
	}

	// errors that should return a 404 status
	resourcesNotFound = map[error]bool{
		errs.ErrDatasetNotFound:  true,
		errs.ErrEditionsNotFound: true,
	}
)

//getDatasets returns a list of datasets, the total count of datasets and an error
func (api *DatasetAPI) getDatasets(w http.ResponseWriter, r *http.Request, limit int, offset int) (interface{}, int, error) {
	ctx := r.Context()
	logData := log.Data{}
	authorised := api.authenticate(r, logData)
	datasets, totalCount, err := api.dataStore.Backend.GetDatasets(ctx, offset, limit, authorised)
	if err != nil {
		log.Error(ctx, "api endpoint getDatasets datastore.GetDatasets returned an error", err)
		handleDatasetAPIErr(ctx, err, w, logData)
		return nil, 0, err
	}
	if authorised {
		return datasets, totalCount, nil
	}
	return mapResults(datasets), totalCount, nil
}

func (api *DatasetAPI) getDataset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	logData := log.Data{"dataset_id": datasetID}

	b, err := func() ([]byte, error) {
		dataset, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			log.Error(ctx, "getDataset endpoint: dataStore.Backend.GetDataset returned an error", err, logData)
			return nil, err
		}

		authorised := api.authenticate(r, logData)

		var b []byte
		var datasetResponse interface{}

		if !authorised {
			// User is not authenticated and hence has only access to current sub document
			if dataset.Current == nil {
				log.Info(ctx, "getDataste endpoint: published dataset not found", logData)
				return nil, errs.ErrDatasetNotFound
			}

			log.Info(ctx, "getDataset endpoint: caller authorised returning dataset current sub document", logData)

			dataset.Current.ID = dataset.ID
			datasetResponse = dataset.Current
		} else {
			// User has valid authentication to get raw dataset document
			if dataset == nil {
				log.Info(ctx, "getDataset endpoint: published or unpublished dataset not found", logData)
				return nil, errs.ErrDatasetNotFound
			}
			log.Info(ctx, "getDataset endpoint: caller not authorised returning dataset", logData)
			datasetResponse = dataset
		}

		b, err = json.Marshal(datasetResponse)
		if err != nil {
			log.Error(ctx, "getDataset endpoint: failed to marshal dataset resource into bytes", err, logData)
			return nil, err
		}

		return b, nil
	}()

	if err != nil {
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	setJSONContentType(w)
	if _, err = w.Write(b); err != nil {
		log.Error(ctx, "getDataset endpoint: error writing bytes to response", err, logData)
		handleDatasetAPIErr(ctx, err, w, logData)
	}
	log.Info(ctx, "getDataset endpoint: request successful", logData)
}

func (api *DatasetAPI) addDataset(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]

	logData := log.Data{"dataset_id": datasetID}

	// TODO Could just do an insert, if dataset already existed we would get a duplicate key error instead of reading then writing doc
	b, err := func() ([]byte, error) {
		_, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			if err != errs.ErrDatasetNotFound {
				log.Error(ctx, "addDataset endpoint: error checking if dataset exists", err, logData)
				return nil, err
			}
		} else {
			log.Error(ctx, "addDataset endpoint: unable to create a dataset that already exists", errs.ErrAddDatasetAlreadyExists, logData)
			return nil, errs.ErrAddDatasetAlreadyExists
		}

		dataset, err := models.CreateDataset(r.Body)
		if err != nil {
			log.Error(ctx, "addDataset endpoint: failed to model dataset resource based on request", err, logData)
			return nil, errs.ErrAddUpdateDatasetBadRequest
		}

		dataType, err := models.ValidateDatasetType(ctx, dataset.Type)
		if err != nil {
			log.Error(ctx, "addDataset endpoint: error Invalid dataset type", err, logData)
			return nil, err
		}

		datasetType, err := models.ValidateNomisURL(ctx, dataType.String(), dataset.NomisReferenceURL)
		if err != nil {
			log.Error(ctx, "addDataset endpoint: error dataset.Type mismatch", err, logData)
			return nil, err
		}

		models.CleanDataset(dataset)

		if err = models.ValidateDataset(dataset); err != nil {
			log.Error(ctx, "addDataset endpoint: dataset failed validation checks", err)
			return nil, err
		}

		dataset.Type = datasetType
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

		// Remove latest version from new dataset resource, this cannot be added at this point
		dataset.Links.LatestVersion = nil

		dataset.LastUpdated = time.Now()

		datasetDoc := &models.DatasetUpdate{
			ID:   datasetID,
			Next: dataset,
		}

		if err = api.dataStore.Backend.UpsertDataset(datasetID, datasetDoc); err != nil {
			logData["new_dataset"] = datasetID
			log.Error(ctx, "addDataset endpoint: failed to insert dataset resource to datastore", err, logData)
			return nil, err
		}

		b, err := json.Marshal(datasetDoc)
		if err != nil {
			log.Error(ctx, "addDataset endpoint: failed to marshal dataset resource into bytes", err, logData)
			return nil, err
		}
		return b, nil
	}()

	if err != nil {
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusCreated)
	if _, err = w.Write(b); err != nil {
		log.Error(ctx, "addDataset endpoint: error writing bytes to response", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Info(ctx, "addDataset endpoint: request completed successfully", logData)
}

func (api *DatasetAPI) putDataset(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	data := log.Data{"dataset_id": datasetID}

	err := func() error {

		dataset, err := models.CreateDataset(r.Body)
		if err != nil {
			log.Error(ctx, "putDataset endpoint: failed to model dataset resource based on request", err, data)
			return errs.ErrAddUpdateDatasetBadRequest
		}

		currentDataset, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			log.Error(ctx, "putDataset endpoint: datastore.getDataset returned an error", err, data)
			return err
		}

		dataset.Type = currentDataset.Next.Type

		_, err = models.ValidateNomisURL(ctx, dataset.Type, dataset.NomisReferenceURL)
		if err != nil {
			log.Error(ctx, "putDataset endpoint: error dataset.Type mismatch", err, data)
			return err
		}

		models.CleanDataset(dataset)

		if err = models.ValidateDataset(dataset); err != nil {
			log.Error(ctx, "putDataset endpoint: failed validation check to update dataset", err, data)
			return err
		}

		if dataset.State == models.PublishedState {
			if err := api.publishDataset(ctx, currentDataset, nil); err != nil {
				log.Error(ctx, "putDataset endpoint: failed to update dataset document to published", err, data)
				return err
			}
		} else {
			if err := api.dataStore.Backend.UpdateDataset(ctx, datasetID, dataset, currentDataset.Next.State); err != nil {
				log.Error(ctx, "putDataset endpoint: failed to update dataset resource", err, data)
				return err
			}
		}
		return nil
	}()

	if err != nil {
		handleDatasetAPIErr(ctx, err, w, data)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.Info(ctx, "putDataset endpoint: request successful", data)
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
		log.Error(ctx, "unable to update dataset", err, log.Data{"dataset_id": currentDataset.ID})
		return err
	}

	return nil
}

func (api *DatasetAPI) deleteDataset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	logData := log.Data{"dataset_id": datasetID, "func": "deleteDataset"}

	// attempt to delete the dataset.
	err := func() error {
		currentDataset, err := api.dataStore.Backend.GetDataset(datasetID)
		if err == errs.ErrDatasetNotFound {
			log.Info(ctx, "cannot delete dataset, it does not exist", logData)
			return errs.ErrDeleteDatasetNotFound
		}
		if err != nil {
			log.Error(ctx, "failed to run query for existing dataset", err, logData)
			return err
		}

		if currentDataset.Current != nil && currentDataset.Current.State == models.PublishedState {
			log.Error(ctx, "unable to delete a published dataset", errs.ErrDeletePublishedDatasetForbidden, logData)
			return errs.ErrDeletePublishedDatasetForbidden
		}

		// Find any editions associated with this dataset
		editionDocs, _, err := api.dataStore.Backend.GetEditions(ctx, currentDataset.ID, "", 0, 0, true)
		if err != nil && err != errs.ErrEditionNotFound {
			return fmt.Errorf("failed to get editions: %w", err)
		}

		if len(editionDocs) == 0 {
			log.Info(ctx, "no editions found for dataset", logData)
		}

		// Then delete them
		for i := range editionDocs {
			if err := api.dataStore.Backend.DeleteEdition(editionDocs[i].ID); err != nil {
				log.Error(ctx, "failed to delete edition", err, logData)
				return err
			}
		}

		if err := api.dataStore.Backend.DeleteDataset(datasetID); err != nil {
			log.Error(ctx, "failed to delete dataset", err, logData)
			return err
		}

		log.Info(ctx, "dataset deleted successfully", logData)
		return nil
	}()

	if err != nil {
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	log.Info(ctx, "delete dataset", logData)
}

// utility function to cut a slice according to the provided offset and limit.
// limit=0 means no limit, and values higher than the slice length are ignored
func slice(full []string, offset, limit int) (sliced []string) {
	end := offset + limit
	if limit == 0 || end > len(full) {
		end = len(full)
	}

	if offset > len(full) {
		return []string{}
	}
	return full[offset:end]
}

func mapResults(results []*models.DatasetUpdate) []*models.Dataset {
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
	case datasetsNoContent[err]:
		status = http.StatusNoContent
	case datasetsBadRequest[err], strings.HasPrefix(err.Error(), "invalid fields:"):
		status = http.StatusBadRequest
	case resourcesNotFound[err]:
		status = http.StatusNotFound
	default:
		err = errs.ErrInternalServer
		status = http.StatusInternalServerError
	}

	data["responseStatus"] = status
	log.Error(ctx, "request unsuccessful", err, data)
	http.Error(w, err.Error(), status)
}
