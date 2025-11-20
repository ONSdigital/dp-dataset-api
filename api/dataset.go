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
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/dp-dataset-api/utils"
	dphttp "github.com/ONSdigital/dp-net/v3/http"
	"github.com/ONSdigital/dp-net/v3/links"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
)

var (
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

	// errors that should return a 403 status
	datasetsForbidden = map[error]bool{
		errs.ErrDeletePublishedDatasetForbidden: true,
	}

	// errors that should return a 404 status
	resourcesNotFound = map[error]bool{
		errs.ErrDatasetNotFound:  true,
		errs.ErrEditionsNotFound: true,
		errs.ErrEditionNotFound:  true,
	}

	// errors that should return a 409 status
	datasetsConflict = map[error]bool{
		errs.ErrAddDatasetAlreadyExists:      true,
		errs.ErrAddDatasetTitleAlreadyExists: true,
	}
)

const IsBasedOn = "is_based_on"
const DatasetType = "type"
const SortOrder = "sort_order"
const DatasetID = "id"

// getDatasets returns a list of datasets, the total count of datasets and an error
func (api *DatasetAPI) getDatasets(w http.ResponseWriter, r *http.Request, limit, offset int) (mappedDatasets interface{}, totalCount int, err error) {
	ctx := r.Context()
	logData := log.Data{}
	authorised := api.authenticate(r, logData)

	isBasedOnExists := r.URL.Query().Has(IsBasedOn)
	isBasedOn := r.URL.Query().Get(IsBasedOn)

	isDatasetTypeExists := r.URL.Query().Has(DatasetType)
	datasetType := r.URL.Query().Get(DatasetType)

	isSortOrderExists := r.URL.Query().Has(SortOrder)
	sortOrder := r.URL.Query().Get(SortOrder)

	isSearchByIDExist := r.URL.Query().Has(DatasetID)
	datasetID := r.URL.Query().Get(DatasetID)

	if isBasedOnExists && isBasedOn == "" {
		err := errs.ErrInvalidQueryParameter
		log.Error(ctx, "malformed is_based_on parameter", err)
		handleDatasetAPIErr(ctx, err, w, logData)
		return nil, 0, err
	}

	if isDatasetTypeExists && datasetType == "" {
		err := errs.ErrInvalidQueryParameter
		log.Error(ctx, "malformed type parameter", err)
		handleDatasetAPIErr(ctx, err, w, logData)
		return nil, 0, err
	}

	if isSortOrderExists && sortOrder != mongo.ASCOrder && sortOrder != mongo.DESCOrder {
		err := errs.ErrInvalidQueryParameter
		log.Error(ctx, "malformed sort_order parameter", err)
		handleDatasetAPIErr(ctx, err, w, logData)
		return nil, 0, err
	}

	if isSearchByIDExist && datasetID == "" {
		err := errs.ErrInvalidQueryParameter
		log.Error(ctx, "malformed dataset_id parameter", err)
		handleDatasetAPIErr(ctx, err, w, logData)
		return nil, 0, err
	}

	var datasets []*models.DatasetUpdate

	if isBasedOnExists || isDatasetTypeExists || isSortOrderExists || isSearchByIDExist {
		datasets, totalCount, err = api.dataStore.Backend.GetDatasetsByQueryParams(ctx, isBasedOn, datasetType, sortOrder, datasetID, offset, limit, authorised)
	} else {
		datasets, totalCount, err = api.dataStore.Backend.GetDatasets(
			ctx,
			offset,
			limit,
			authorised,
		)
	}
	if err != nil {
		log.Error(ctx, "api endpoint getDatasets datastore.GetDatasets returned an error", err)
		handleDatasetAPIErr(ctx, err, w, logData)
		return nil, 0, err
	}

	if api.enableURLRewriting {
		datasetLinksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetDatasetAPIURL())

		if authorised {
			datasetsResponse, err := utils.RewriteDatasetsWithAuth(ctx, datasets, datasetLinksBuilder)
			if err != nil {
				log.Error(ctx, "getDatasets endpoint: failed to rewrite datasets with auth", err)
				handleDatasetAPIErr(ctx, err, w, logData)
				return nil, 0, err
			}
			log.Info(ctx, "getDatasets endpoint: get all datasets with auth", logData)
			return datasetsResponse, totalCount, nil
		}

		datasetsResponse, err := utils.RewriteDatasetsWithoutAuth(ctx, datasets, datasetLinksBuilder)
		if err != nil {
			log.Error(ctx, "getDatasets endpoint: failed to rewrite datasets without authorisation", err)
			handleDatasetAPIErr(ctx, err, w, logData)
			return nil, 0, err
		}
		log.Info(ctx, "getDatasets endpoint: get all datasets without auth", logData)
		return datasetsResponse, totalCount, nil
	}

	if authorised {
		return datasets, totalCount, nil
	}

	return mapResults(datasets), totalCount, nil
}

//nolint:gocognit // cognitive complexity (> 30) is acceptable for now
func (api *DatasetAPI) getDataset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	logData := log.Data{"dataset_id": datasetID}

	b, err := func() ([]byte, error) {
		dataset, err := api.dataStore.Backend.GetDataset(ctx, datasetID)
		if err != nil {
			log.Error(ctx, "getDataset endpoint: dataStore.Backend.GetDataset returned an error", err, logData)
			return nil, err
		}

		authorised := api.authenticate(r, logData)

		datasetLinksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetDatasetAPIURL())

		var datasetResponse interface{}

		if api.enableURLRewriting {
			if authorised {
				datasetResponse, err = utils.RewriteDatasetWithAuth(ctx, dataset, datasetLinksBuilder)
				if err != nil {
					log.Error(ctx, "getDataset endpoint: failed to rewrite dataset with authorisation", err, logData)
					return nil, err
				}
				log.Info(ctx, "getDataset endpoint: get dataset with auth", logData)
			} else {
				datasetResponse, err = utils.RewriteDatasetWithoutAuth(ctx, dataset, datasetLinksBuilder)
				if err != nil {
					log.Error(ctx, "getDataset endpoint: failed to rewrite dataset without authorisation", err, logData)
					return nil, err
				}
				log.Info(ctx, "getDataset endpoint: get dataset without auth", logData)
			}
		} else {
			if !authorised {
				// User is not authenticated and hence has only access to current sub document
				if dataset.Current == nil {
					log.Info(ctx, "getDataset endpoint: published dataset not found", logData)
					return nil, errs.ErrDatasetNotFound
				}
				log.Info(ctx, "getDataset endpoint: caller not authorised returning dataset", logData)

				dataset.Current.ID = dataset.ID

				if dataset.Current.Type != models.Static.String() && dataset.Current.Topics == nil {
					dataset.Current.Topics = nil
				}

				datasetResponse = dataset.Current
			} else {
				// User has valid authentication to get raw dataset document
				if dataset == nil {
					log.Info(ctx, "getDataset endpoint: published or unpublished dataset not found", logData)
					return nil, errs.ErrDatasetNotFound
				}
				log.Info(ctx, "getDataset endpoint: caller authorised returning dataset current sub document", logData)

				if dataset.Current != nil && dataset.Current.Type != models.Static.String() && dataset.Current.Topics == nil {
					dataset.Current.Topics = nil
				}

				if dataset.Next != nil && dataset.Next.Type != models.Static.String() && dataset.Next.Topics == nil {
					dataset.Next.Topics = nil
				}

				datasetResponse = dataset
			}
		}

		b, err := json.Marshal(datasetResponse)
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
		dataset, err := models.CreateDataset(r.Body)
		if err != nil {
			log.Error(ctx, "addDataset endpoint: failed to model dataset resource based on request", err, logData)
			return nil, errs.ErrAddUpdateDatasetBadRequest
		}

		models.CleanDataset(dataset)
		if err = models.ValidateDataset(dataset); err != nil {
			log.Error(ctx, "addDataset endpoint: dataset failed validation checks", err)
			return nil, err
		}

		_, err = api.dataStore.Backend.GetDataset(ctx, datasetID)
		if err != nil {
			if err != errs.ErrDatasetNotFound {
				log.Error(ctx, "addDataset endpoint: error checking if dataset exists", err, logData)
				return nil, err
			}
		} else {
			log.Error(ctx, "addDataset endpoint: unable to create a dataset that already exists", errs.ErrAddDatasetAlreadyExists, logData)
			return nil, errs.ErrAddDatasetAlreadyExists
		}

		dataType, err := models.ValidateDatasetType(ctx, dataset.Type)
		if err != nil {
			log.Error(ctx, "addDataset endpoint: error Invalid dataset type", err, logData)
			return nil, err
		}

		dataset.Type = dataType.String()
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

		if err = api.dataStore.Backend.UpsertDataset(ctx, datasetID, datasetDoc); err != nil {
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

func (api *DatasetAPI) addDatasetNew(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()

	dataset, err := models.CreateDataset(r.Body)
	if err != nil {
		log.Error(ctx, "addDatasetNew endpoint: failed to model dataset resource based on request", err, nil)
		handleDatasetAPIErr(ctx, errs.ErrAddUpdateDatasetBadRequest, w, nil)
		return
	}

	datasetID := dataset.ID
	logData := log.Data{"dataset_id": datasetID}

	models.CleanDataset(dataset)
	if err = models.ValidateDataset(dataset); err != nil {
		log.Error(ctx, "addDatasetNew endpoint: dataset failed validation checks", err)
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	if datasetID == "" {
		log.Error(ctx, "addDatasetNew endpoint: dataset ID is empty", nil)
		handleDatasetAPIErr(ctx, errs.ErrMissingDatasetID, w, nil)
		return
	}

	_, err = api.dataStore.Backend.GetDataset(ctx, datasetID)
	if err == nil {
		log.Error(ctx, "addDatasetNew endpoint: unable to create a dataset that already exists", errs.ErrAddDatasetAlreadyExists, logData)
		handleDatasetAPIErr(ctx, errs.ErrAddDatasetAlreadyExists, w, logData)
		return
	}
	if err != errs.ErrDatasetNotFound {
		log.Error(ctx, "addDatasetNew endpoint: error checking if dataset exists", err, logData)
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	datasetTitleExist, err := api.dataStore.Backend.CheckDatasetTitleExist(ctx, dataset.Title)
	if err != nil {
		log.Error(ctx, "addDatasetNew endpoint: error checking if dataset title exists", err, logData)
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	if datasetTitleExist {
		log.Error(ctx, "addDatasetNew endpoint: unable to create a dataset with title that already exists", errs.ErrAddDatasetTitleAlreadyExists, logData)
		handleDatasetAPIErr(ctx, errs.ErrAddDatasetTitleAlreadyExists, w, logData)
		return
	}

	dataType, err := models.ValidateDatasetType(ctx, dataset.Type)
	if err != nil {
		log.Error(ctx, "addDatasetNew endpoint: error Invalid dataset type", err, logData)
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	dataset.Type = dataType.String()
	dataset.State = models.CreatedState

	if dataset.Links == nil {
		dataset.Links = &models.DatasetLinks{}
	}

	dataset.Links.Editions = &models.LinkObject{
		HRef: fmt.Sprintf("%s/datasets/%s/editions", api.host, datasetID),
	}

	dataset.Links.Self = &models.LinkObject{
		HRef: fmt.Sprintf("%s/datasets/%s", api.host, datasetID),
	}

	dataset.Links.LatestVersion = nil

	dataset.LastUpdated = time.Now()

	datasetDoc := &models.DatasetUpdate{
		ID:   datasetID,
		Next: dataset,
	}

	if err = api.dataStore.Backend.UpsertDataset(ctx, datasetID, datasetDoc); err != nil {
		logData["new_dataset"] = datasetID
		log.Error(ctx, "addDatasetNew endpoint: failed to insert dataset resource to datastore", err, logData)
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	b, err := json.Marshal(datasetDoc)
	if err != nil {
		log.Error(ctx, "addDatasetNew endpoint: failed to marshal dataset resource into bytes", err, logData)
		handleDatasetAPIErr(ctx, err, w, logData)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusCreated)
	if _, err = w.Write(b); err != nil {
		log.Error(ctx, "addDatasetNew endpoint: error writing bytes to response", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Info(ctx, "addDatasetNew endpoint: request completed successfully", logData)
}

func (api *DatasetAPI) putDataset(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	data := log.Data{"dataset_id": datasetID}

	b, err := func() ([]byte, error) {
		dataset, err := models.CreateDataset(r.Body)
		if err != nil {
			log.Error(ctx, "putDataset endpoint: failed to model dataset resource based on request", err, data)
			return nil, errs.ErrAddUpdateDatasetBadRequest
		}

		currentDataset, err := api.dataStore.Backend.GetDataset(ctx, datasetID)
		if err != nil {
			log.Error(ctx, "putDataset endpoint: datastore.getDataset returned an error", err, data)
			return nil, err
		}

		dataset.Type = currentDataset.Next.Type

		models.CleanDataset(dataset)

		if err = models.ValidateDataset(dataset); err != nil {
			log.Error(ctx, "putDataset endpoint: failed validation check to update dataset", err, data)
			return nil, err
		}

		if dataset.Type == models.Static.String() {
			datasetTitleExists, err := api.dataStore.Backend.CheckDatasetTitleExist(ctx, dataset.Title)
			if err != nil {
				log.Error(ctx, "putDataset endpoint: error checking if dataset title exists", err, data)
				return nil, err
			}

			if datasetTitleExists && dataset.Title != currentDataset.Next.Title {
				log.Error(ctx, "putDataset endpoint: unable to update a dataset with title that already exists", errs.ErrAddDatasetTitleAlreadyExists, data)
				return nil, errs.ErrAddDatasetTitleAlreadyExists
			}
		}

		if dataset.State == models.PublishedState {
			if err := api.publishDataset(ctx, currentDataset, nil); err != nil {
				log.Error(ctx, "putDataset endpoint: failed to update dataset document to published", err, data)
				return nil, err
			}
		} else {
			if err := api.dataStore.Backend.UpdateDataset(ctx, datasetID, dataset, currentDataset.Next.State); err != nil {
				log.Error(ctx, "putDataset endpoint: failed to update dataset resource", err, data)
				return nil, err
			}
		}

		b, err := json.Marshal(dataset)
		if err != nil {
			log.Error(ctx, "putDataset endpoint: failed to marshal dataset resource into bytes", err, data)
			return nil, err
		}
		return b, nil
	}()

	if err != nil {
		handleDatasetAPIErr(ctx, err, w, data)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	if _, err = w.Write(b); err != nil {
		log.Error(ctx, "putDataset endpoint: error writing bytes to response", err, data)
		handleDatasetAPIErr(ctx, err, w, data)
	}
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

	if err := api.dataStore.Backend.UpsertDataset(ctx, currentDataset.ID, newDataset); err != nil {
		log.Error(ctx, "unable to update dataset", err, log.Data{"dataset_id": currentDataset.ID})
		return err
	}

	return nil
}

//nolint:gocognit,gocyclo // Complexity acceptable for now, refactoring can be done later if needed
func (api *DatasetAPI) deleteDataset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	logData := log.Data{"dataset_id": datasetID, "func": "deleteDataset"}

	// attempt to delete the dataset.
	err := func() error {
		currentDataset, err := api.dataStore.Backend.GetDataset(ctx, datasetID)
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

		// Find any editions/versions associated with the dataset based on the type
		if currentDataset.Next.Type == models.Static.String() {
			// Limit is set to DEFAULT_LIMIT (20) to prevent unbounded queries.
			// If a dataset has more than DEFAULT_LIMIT unpublished editions/versions, only the first DEFAULT_LIMIT will be deleted.
			// Refactoring is required if more than DEFAULT_LIMIT editions/versions per dataset is a possibility.
			versionDocs, _, err := api.dataStore.Backend.GetAllStaticVersions(ctx, currentDataset.ID, "", 0, api.defaultLimit)
			if err != nil {
				if err == errs.ErrVersionsNotFound {
					log.Info(ctx, "deleteDataset endpoint: dataset didn't contain any versions, continuing to delete dataset", logData)
				} else {
					log.Error(ctx, "deleteDataset endpoint: failed to get versions for static dataset", err, logData)
					return err
				}
			}

			for i := range versionDocs {
				if versionDocs[i].Distributions != nil {
					for _, distribution := range *versionDocs[i].Distributions {
						logData["distribution_title"] = distribution.Title
						logData["distribution_download_url"] = distribution.DownloadURL

						err := api.filesAPIClient.DeleteFile(ctx, distribution.DownloadURL)
						if err != nil {
							log.Error(ctx, "deleteDataset endpoint: failed to delete distribution file from files API", err, logData)
							return err
						}
						log.Info(ctx, "deleteDataset endpoint: successfully deleted distribution file from files API", logData)
					}
				}

				err := api.dataStore.Backend.DeleteStaticDatasetVersion(ctx, currentDataset.ID, versionDocs[i].Edition, versionDocs[i].Version)
				if err != nil {
					log.Error(ctx, "deleteDataset endpoint: failed to delete version", err, logData)
					return err
				}
			}
		} else {
			editionDocs, _, err := api.dataStore.Backend.GetEditions(ctx, currentDataset.ID, "", 0, 0, true)
			if err != nil && err != errs.ErrEditionNotFound {
				return fmt.Errorf("failed to get editions: %w", err)
			}

			if len(editionDocs) == 0 {
				log.Info(ctx, "no editions found for dataset", logData)
			}

			for i := range editionDocs {
				if err := api.dataStore.Backend.DeleteEdition(ctx, editionDocs[i].ID); err != nil {
					log.Error(ctx, "failed to delete edition", err, logData)
					return err
				}
			}
		}

		if err := api.dataStore.Backend.DeleteDataset(ctx, datasetID); err != nil {
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
	case datasetsConflict[err]:
		status = http.StatusConflict
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
