package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/utils"
	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/log.go/log"
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

func (api *DatasetAPI) getDatasets(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logData := log.Data{}
	offsetParameter := r.URL.Query().Get("offset")
	limitParameter := r.URL.Query().Get("limit")

	offset := api.defaultOffset
	limit := api.defaultLimit

	var err error

	if offsetParameter != "" {
		logData["offset"] = offsetParameter
		offset, err = utils.ValidatePositiveInt(offsetParameter)
		if err != nil {
			log.Event(ctx, "failed to obtain a positive integer value for offset query parameter", log.ERROR)
			handleDatasetAPIErr(ctx, err, w, nil)
			return
		}
	}

	if limitParameter != "" {
		logData["limit"] = limitParameter
		limit, err = utils.ValidatePositiveInt(limitParameter)
		if err != nil {
			log.Event(ctx, "failed to obtain a positive integer value for limit query parameter", log.ERROR)
			handleDatasetAPIErr(ctx, err, w, nil)
			return
		}
	}

	if limit > api.maxLimit {
		logData["max_limit"] = api.maxLimit
		err = errs.ErrInvalidQueryParameter
		log.Event(ctx, "limit is greater than the maximum allowed", log.ERROR, logData)
		handleDatasetAPIErr(ctx, err, w, nil)
		return
	}

	b, err := func() ([]byte, error) {

		logData := log.Data{}

		authorised := api.authenticate(r, logData)

		datasets, err := api.dataStore.Backend.GetDatasets(ctx, offset, limit, authorised)
		if err != nil {
			log.Event(ctx, "api endpoint getDatasets datastore.GetDatasets returned an error", log.ERROR, log.Error(err))
			return nil, err
		}

		var b []byte

		var datasetsResponse interface{}

		if authorised {
			datasetsResponse = datasets
		} else {
			datasetsResponse = &models.DatasetResults{
				Items:      mapResults(datasets.Items),
				Offset:     offset,
				Limit:      limit,
				Count:      datasets.Count,
				TotalCount: datasets.TotalCount,
			}

		}

		b, err = json.Marshal(datasetsResponse)

		if err != nil {
			log.Event(ctx, "api endpoint getDatasets failed to marshal dataset resource into bytes", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		return b, nil
	}()

	if err != nil {
		handleDatasetAPIErr(ctx, err, w, nil)
		return
	}

	setJSONContentType(w)
	if _, err = w.Write(b); err != nil {
		log.Event(ctx, "api endpoint getDatasets error writing response body", log.ERROR, log.Error(err))
		handleDatasetAPIErr(ctx, err, w, nil)
		return
	}
	log.Event(ctx, "api endpoint getDatasets request successful", log.INFO)
}

func (api *DatasetAPI) getDataset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	logData := log.Data{"dataset_id": datasetID}

	b, err := func() ([]byte, error) {
		dataset, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			log.Event(ctx, "getDataset endpoint: dataStore.Backend.GetDataset returned an error", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		authorised := api.authenticate(r, logData)

		var b []byte
		var datasetResponse interface{}

		if !authorised {
			// User is not authenticated and hence has only access to current sub document
			if dataset.Current == nil {
				log.Event(ctx, "getDataste endpoint: published dataset not found", log.INFO, logData)
				return nil, errs.ErrDatasetNotFound
			}

			log.Event(ctx, "getDataset endpoint: caller authorised returning dataset current sub document", log.INFO, logData)

			dataset.Current.ID = dataset.ID
			datasetResponse = dataset.Current
		} else {
			// User has valid authentication to get raw dataset document
			if dataset == nil {
				log.Event(ctx, "getDataset endpoint: published or unpublished dataset not found", log.INFO, logData)
				return nil, errs.ErrDatasetNotFound
			}
			log.Event(ctx, "getDataset endpoint: caller not authorised returning dataset", log.INFO, logData)
			datasetResponse = dataset
		}

		b, err = json.Marshal(datasetResponse)
		if err != nil {
			log.Event(ctx, "getDataset endpoint: failed to marshal dataset resource into bytes", log.ERROR, log.Error(err), logData)
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
		log.Event(ctx, "getDataset endpoint: error writing bytes to response", log.ERROR, log.Error(err), logData)
		handleDatasetAPIErr(ctx, err, w, logData)
	}
	log.Event(ctx, "getDataset endpoint: request successful", log.INFO, logData)
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
				log.Event(ctx, "addDataset endpoint: error checking if dataset exists", log.ERROR, log.Error(err), logData)
				return nil, err
			}
		} else {
			log.Event(ctx, "addDataset endpoint: unable to create a dataset that already exists", log.ERROR, log.Error(errs.ErrAddDatasetAlreadyExists), logData)
			return nil, errs.ErrAddDatasetAlreadyExists
		}

		dataset, err := models.CreateDataset(r.Body)
		if err != nil {
			log.Event(ctx, "addDataset endpoint: failed to model dataset resource based on request", log.ERROR, log.Error(err), logData)
			return nil, errs.ErrAddUpdateDatasetBadRequest
		}

		dataType, err := models.ValidateDatasetType(ctx, dataset.Type)
		if err != nil {
			log.Event(ctx, "addDataset endpoint: error Invalid dataset type", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		datasetType, err := models.ValidateNomisURL(ctx, dataType.String(), dataset.NomisReferenceURL)
		if err != nil {
			log.Event(ctx, "addDataset endpoint: error dataset.Type mismatch", log.ERROR, log.Error(err), logData)
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
			log.Event(ctx, "addDataset endpoint: failed to insert dataset resource to datastore", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		b, err := json.Marshal(datasetDoc)
		if err != nil {
			log.Event(ctx, "addDataset endpoint: failed to marshal dataset resource into bytes", log.ERROR, log.Error(err), logData)
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
		log.Event(ctx, "addDataset endpoint: error writing bytes to response", log.ERROR, log.Error(err), logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Event(ctx, "addDataset endpoint: request completed successfully", log.INFO, logData)
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
			log.Event(ctx, "putDataset endpoint: failed to model dataset resource based on request", log.ERROR, log.Error(err), data)
			return errs.ErrAddUpdateDatasetBadRequest
		}

		currentDataset, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			log.Event(ctx, "putDataset endpoint: datastore.getDataset returned an error", log.ERROR, log.Error(err), data)
			return err
		}

		dataset.Type = currentDataset.Next.Type

		_, err = models.ValidateNomisURL(ctx, dataset.Type, dataset.NomisReferenceURL)
		if err != nil {
			log.Event(ctx, "putDataset endpoint: error dataset.Type mismatch", log.ERROR, log.Error(err), data)
			return err
		}

		if dataset.State == models.PublishedState {
			if err := api.publishDataset(ctx, currentDataset, nil); err != nil {
				log.Event(ctx, "putDataset endpoint: failed to update dataset document to published", log.ERROR, log.Error(err), data)
				return err
			}
		} else {
			if err := api.dataStore.Backend.UpdateDataset(ctx, datasetID, dataset, currentDataset.Next.State); err != nil {
				log.Event(ctx, "putDataset endpoint: failed to update dataset resource", log.ERROR, log.Error(err), data)
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
	log.Event(ctx, "putDataset endpoint: request successful", log.INFO, data)
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
		log.Event(ctx, "unable to update dataset", log.ERROR, log.Error(err), log.Data{"dataset_id": currentDataset.ID})
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
			log.Event(ctx, "cannot delete dataset, it does not exist", log.INFO, logData)
			return errs.ErrDeleteDatasetNotFound
		}

		if err != nil {
			log.Event(ctx, "failed to run query for existing dataset", log.ERROR, log.Error(err), logData)
			return err
		}

		if currentDataset.Current != nil && currentDataset.Current.State == models.PublishedState {
			log.Event(ctx, "unable to delete a published dataset", log.ERROR, log.Error(errs.ErrDeletePublishedDatasetForbidden), logData)
			return errs.ErrDeletePublishedDatasetForbidden
		}

		// Find any editions associated with this dataset
		editionDocs, err := api.dataStore.Backend.GetEditions(ctx, currentDataset.ID, "", 0, 0, true)
		if err != nil {
			log.Event(ctx, "unable to find the dataset editions", log.ERROR, log.Error(errs.ErrEditionsNotFound), logData)
			return errs.ErrEditionsNotFound
		}

		// Then delete them
		for i := range editionDocs.Items {
			if err := api.dataStore.Backend.DeleteEdition(editionDocs.Items[i].ID); err != nil {
				log.Event(ctx, "failed to delete edition", log.ERROR, log.Error(err), logData)
				return err
			}
		}

		if err := api.dataStore.Backend.DeleteDataset(datasetID); err != nil {
			log.Event(ctx, "failed to delete dataset", log.ERROR, log.Error(err), logData)
			return err
		}
		log.Event(ctx, "dataset deleted successfully", log.INFO, logData)
		return nil
	}()

	if err != nil {
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	log.Event(ctx, "delete dataset", log.INFO, logData)
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
	case datasetsNoContent[err]:
		status = http.StatusNoContent
	case datasetsBadRequest[err]:
		status = http.StatusBadRequest
	case resourcesNotFound[err]:
		status = http.StatusNotFound
	default:
		err = errs.ErrInternalServer
		status = http.StatusInternalServerError
	}

	data["responseStatus"] = status
	log.Event(ctx, "request unsuccessful", log.ERROR, log.Error(err), data)
	http.Error(w, err.Error(), status)
}
