package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

const (
	internalToken = "Internal-Token"

	datasetDocType         = "dataset"
	editionDocType         = "edition"
	versionDocType         = "version"
	instanceDocType        = "instance"
	dimensionDocType       = "dimension"
	dimensionOptionDocType = "dimension-option"
)

type GenerateVersionDownloads struct {
	FilterID   string `avro:"filter_output_id"`
	InstanceID string `avro:"instance_id"`
	DatasetID  string `avro:"dataset_id"`
	Edition    string `avro:"edition"`
	Version    string `avro:"version"`
}

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
			log.ErrorC("fail to marshal dataset resource into bytes", err, nil)
			handleErrorType(datasetDocType, err, w)
			return
		}
	} else {
		datasets := &models.DatasetResults{}

		datasets.Items = mapResults(results)

		bytes, err = json.Marshal(datasets)
		if err != nil {
			log.ErrorC("fail to marshal dataset resource into bytes", err, nil)
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
			log.Debug("published dataset not found", nil)
			handleErrorType(datasetDocType, errs.ErrDatasetNotFound, w)
			return
		}

		dataset.Current.ID = dataset.ID
		bytes, err = json.Marshal(dataset.Current)
		if err != nil {
			log.ErrorC("fail to marshal dataset current sub document resource into bytes", err, log.Data{"dataset_id": id})
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		if dataset == nil {
			log.Debug("published or unpublished dataset not found", nil)
			handleErrorType(datasetDocType, errs.ErrDatasetNotFound, w)
		}
		bytes, err = json.Marshal(dataset)
		if err != nil {
			log.ErrorC("fail to marshal dataset current sub document resource into bytes", err, log.Data{"dataset_id": id})
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
		state = models.PublishedState
	}

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.ErrorC("unable to find dataset", err, log.Data{"dataset_id": id})
		handleErrorType(editionDocType, err, w)
		return
	}

	results, err := api.dataStore.Backend.GetEditions(id, state)
	if err != nil {
		log.ErrorC("unable to find editions for dataset", err, log.Data{"dataset_id": id})
		handleErrorType(editionDocType, err, w)
		return
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		log.ErrorC("fail to marshal a list of edition resources into bytes", err, log.Data{"dataset_id": id})
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
		state = models.PublishedState
	}

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.ErrorC("unable to find dataset", err, log.Data{"dataset_id": id, "edition": editionID})
		handleErrorType(editionDocType, err, w)
		return
	}

	edition, err := api.dataStore.Backend.GetEdition(id, editionID, state)
	if err != nil {
		log.ErrorC("unable to find edition", err, log.Data{"dataset_id": id, "edition": editionID})
		handleErrorType(editionDocType, err, w)
		return
	}

	bytes, err := json.Marshal(edition)
	if err != nil {
		log.ErrorC("fail to marshal edition resource into bytes", err, log.Data{"dataset_id": id, "edition": editionID})
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
		state = models.PublishedState
	}

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.ErrorC("fail to find dataset for list of versions", err, log.Data{"dataset_id": id, "edition": editionID})
		handleErrorType(versionDocType, err, w)
		return
	}

	if err := api.dataStore.Backend.CheckEditionExists(id, editionID, state); err != nil {
		log.ErrorC("fail to find edition for list of versions", err, log.Data{"dataset_id": id, "edition": editionID})
		handleErrorType(versionDocType, err, w)
		return
	}

	results, err := api.dataStore.Backend.GetVersions(id, editionID, state)
	if err != nil {
		log.ErrorC("fail to find any versions for dataset edition", err, log.Data{"dataset_id": id, "edition": editionID})
		handleErrorType(versionDocType, err, w)
		return
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		log.ErrorC("fail to marshal list of version resources into bytes", err, log.Data{"dataset_id": id, "edition": editionID})
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
		state = models.PublishedState
	}

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.ErrorC("fail to find dataset", err, log.Data{"dataset_id": id, "edition": editionID, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	if err := api.dataStore.Backend.CheckEditionExists(id, editionID, state); err != nil {
		log.ErrorC("fail to find edition for dataset", err, log.Data{"dataset_id": id, "edition": editionID, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	results, err := api.dataStore.Backend.GetVersion(id, editionID, version, state)
	if err != nil {
		log.ErrorC("fail to find version for dataset edition", err, log.Data{"dataset_id": id, "edition": editionID, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	results.Links.Self.HRef = results.Links.Version.HRef

	bytes, err := json.Marshal(results)
	if err != nil {
		log.ErrorC("fail to marshal version resource into bytes", err, log.Data{"dataset_id": id, "edition": editionID, "version": version})
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
		if err != errs.ErrDatasetNotFound {
			log.ErrorC("fail to find dataset", err, log.Data{"dataset_id": datasetID})
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
		log.ErrorC("fail to model dataset resource based on request", err, log.Data{"dataset_id": datasetID})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	dataset.State = models.CreatedState

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

	if err = api.dataStore.Backend.UpsertDataset(datasetID, datasetDoc); err != nil {
		log.ErrorC("fail to insert dataset resource to datastore", err, log.Data{"new_dataset": datasetID})
		handleErrorType(datasetDocType, err, w)
		return
	}

	bytes, err := json.Marshal(datasetDoc)
	if err != nil {
		log.ErrorC("fail to marshal dataset resource into bytes", err, log.Data{"new_dataset": datasetID})
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
		log.ErrorC("fail to model dataset resource based on request", err, log.Data{"dataset_id": datasetID})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err := api.dataStore.Backend.UpdateDataset(datasetID, dataset); err != nil {
		log.ErrorC("failed to update dataset resource", err, log.Data{"dataset_id": datasetID})
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
	edition := vars["edition"]
	version := vars["version"]

	versionDoc, err := models.CreateVersion(r.Body)
	if err != nil {
		log.ErrorC("fail to model version resource based on request", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if err = api.dataStore.Backend.CheckDatasetExists(datasetID, ""); err != nil {
		log.ErrorC("fail to find dataset", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	if err = api.dataStore.Backend.CheckEditionExists(datasetID, edition, ""); err != nil {
		log.ErrorC("fail to find edition of dataset", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	currentVersion, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, "")
	if err != nil {
		log.ErrorC("fail to find version of dataset edition", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	// Check current state of version document
	if currentVersion.State == models.PublishedState {
		err = fmt.Errorf("unable to update document, already published")
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	// Combine update version document to existing version document
	newVersion := createNewVersionDoc(currentVersion, versionDoc)
	log.Debug("combined current version document with update request", log.Data{"dataset_id": datasetID, "edition": edition, "version": version, "updated_version": newVersion})

	if err = models.ValidateVersion(newVersion); err != nil {
		log.ErrorC("failed validation check for version update", err, nil)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := api.dataStore.Backend.UpdateVersion(newVersion.ID, versionDoc); err != nil {
		log.ErrorC("failed to update version document", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	if versionDoc.State == models.PublishedState {
		if err := api.dataStore.Backend.UpdateEdition(datasetID, edition, versionDoc); err != nil {
			log.ErrorC("failed to update the state of edition document to published", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
			handleErrorType(versionDocType, err, w)
			return
		}

		// Pass in newVersion variable to include relevant data needed for update on dataset API (e.g. links)
		if err := api.publishDataset(datasetID, newVersion); err != nil {
			log.ErrorC("failed to update dataset document once version state changes to publish", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
			handleErrorType(versionDocType, err, w)
			return
		}
	}

	if versionDoc.State == models.AssociatedState {
		if err := api.dataStore.Backend.UpdateDatasetWithAssociation(datasetID, versionDoc.State, versionDoc); err != nil {
			log.ErrorC("failed to update dataset document after a version of a dataset has been associated with a collection", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
			handleErrorType(versionDocType, err, w)
			return
		}

		if err := api.downloadGenerator.Generate(datasetID, versionDoc.ID, edition, version); err != nil {
			err = errors.Wrap(err, "error while attempting to generate full dataset version downloads")
			log.Error(err, log.Data{
				"dataset_id":  datasetID,
				"instance_id": versionDoc.ID,
				"edition":     edition,
				"version":     version,
			})
			handleErrorType(versionDocType, err, w)
		}
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.Debug("update dataset", log.Data{"dataset_id": datasetID})
}

func (api *DatasetAPI) PutVersionDownloads(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	edition := vars["edition"]
	version := vars["version"]

	updatedDownloads, err := models.CreateDownloadList(r.Body)
	if err != nil {
		err = errors.Wrap(err, "failed to unmarshal put version downloads request")
		log.Error(err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	defer r.Body.Close()

	if err = api.dataStore.Backend.CheckDatasetExists(datasetID, ""); err != nil {
		log.ErrorC("fail to find dataset", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	if err = api.dataStore.Backend.CheckEditionExists(datasetID, edition, ""); err != nil {
		log.ErrorC("fail to find edition of dataset", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	currentVersion, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, "")
	if err != nil {
		log.ErrorC("fail to find version of dataset edition", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	// Check current state of version document
	if currentVersion.State == models.PublishedState {
		err = fmt.Errorf("unable to update document, already published")
		log.Error(err, nil)
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	// create a new version doc to hold the updated downloads
	versionDoc := &models.Version{
		Downloads: currentVersion.Downloads,
	}

	if versionDoc.Downloads == nil {
		versionDoc.Downloads = &models.DownloadList{}
	}

	// Update the xls download option
	if updatedDownloads.XLS != nil {
		versionDoc.Downloads.XLS = updatedDownloads.XLS
	}
	// update the csv download option
	if updatedDownloads.CSV != nil {
		versionDoc.Downloads.CSV = updatedDownloads.CSV
	}

	if err := api.dataStore.Backend.UpdateVersion(currentVersion.ID, versionDoc); err != nil {
		log.ErrorC("failed to update version document", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
}

func createNewVersionDoc(currentVersion *models.Version, version *models.Version) *models.Version {

	var alerts []models.Alert
	if currentVersion.Alerts != nil {

		// loop through current alerts and add each alert to array
		for _, currentAlert := range *currentVersion.Alerts {
			alerts = append(alerts, currentAlert)
		}
	}

	if version.Alerts != nil {

		// loop through new alerts and add each alert to array
		for _, newAlert := range *version.Alerts {
			alerts = append(alerts, newAlert)
		}
	}

	if alerts != nil {
		version.Alerts = &alerts
	}

	if version.CollectionID == "" {
		version.CollectionID = currentVersion.CollectionID
	}

	var latestChanges []models.LatestChange
	if currentVersion.LatestChanges != nil {

		// loop through current latestChanges and add each latest change to array
		for _, currentLatestChange := range *currentVersion.LatestChanges {
			latestChanges = append(latestChanges, currentLatestChange)
		}
	}

	if version.LatestChanges != nil {

		// loop through new latestChanges and add each latest change to array
		for _, newLatestChange := range *version.LatestChanges {
			latestChanges = append(latestChanges, newLatestChange)
		}
	}

	if latestChanges != nil {
		version.LatestChanges = &latestChanges
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

func (api *DatasetAPI) publishDataset(id string, version *models.Version) error {
	currentDataset, err := api.dataStore.Backend.GetDataset(id)
	if err != nil {
		log.ErrorC("unable to update dataset", err, log.Data{"dataset_id": id})
		return err
	}

	var accessRights string

	if currentDataset.Next.Links != nil {
		if currentDataset.Next.Links.AccessRights != nil {
			accessRights = currentDataset.Next.Links.AccessRights.HRef
		}
	}

	currentDataset.Next.CollectionID = version.CollectionID
	currentDataset.Next.Links = &models.DatasetLinks{
		AccessRights: &models.LinkObject{
			HRef: accessRights,
		},
		Editions: &models.LinkObject{
			HRef: fmt.Sprintf("%s/datasets/%s/editions", api.host, version.Links.Dataset.ID),
		},
		LatestVersion: &models.LinkObject{
			ID:   version.Links.Version.ID,
			HRef: version.Links.Version.HRef,
		},
		Self: &models.LinkObject{
			HRef: fmt.Sprintf("%s/datasets/%s", api.host, version.Links.Dataset.ID),
		},
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

	if err := api.dataStore.Backend.UpsertDataset(id, newDataset); err != nil {
		log.ErrorC("unable to update dataset", err, log.Data{"dataset_id": id})
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
		log.ErrorC("failed to get version dimensions", err, log.Data{"dataset_id": datasetID, "edition": editionID, "version": versionID})
		handleErrorType(dimensionDocType, err, w)
		return
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		log.ErrorC("fail to marshal list of dimension resources into bytes", err, log.Data{"dataset_id": datasetID, "edition": editionID, "version": versionID})
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
		log.ErrorC("failed to get a list of dimension options", err, log.Data{"dataset_id": datasetID, "edition": editionID, "version": versionID, "dimension": dimension})
		handleErrorType(dimensionOptionDocType, err, w)
		return
	}

	bytes, err := json.Marshal(results)
	if err != nil {
		log.ErrorC("fail to marshal list of dimension option resources into bytes", err, log.Data{"dataset_id": datasetID, "edition": editionID, "version": versionID, "dimension": dimension})
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

func (api *DatasetAPI) getMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	edition := vars["edition"]
	version := vars["version"]

	// get dataset document
	datasetDoc, err := api.dataStore.Backend.GetDataset(datasetID)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	// Default state to published
	var state string

	// if request is authenticated then access resources of state other than published
	if r.Header.Get(internalToken) != api.internalToken {

		// Check for current sub document
		if datasetDoc.Current == nil || datasetDoc.Current.State != models.PublishedState {
			log.ErrorC("found dataset but currently unpublished", errs.ErrDatasetNotFound, log.Data{"dataset_id": datasetID, "edition": edition, "version": version, "dataset": datasetDoc.Current})
			http.Error(w, errs.ErrDatasetNotFound.Error(), http.StatusBadRequest)
			return
		}

		state = datasetDoc.Current.State
	}

	if err = api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
		log.ErrorC("fail to find edition for dataset", err, log.Data{"dataset_id": datasetID, "edition": edition, version: version})
		handleErrorType(versionDocType, err, w)
		return
	}

	versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, state)
	if err != nil {
		log.ErrorC("fail to find version for dataset edition", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	var metaDataDoc *models.Metadata
	// combine version and dataset metadata
	if state != models.PublishedState && versionDoc.CollectionID == datasetDoc.Next.CollectionID {
		metaDataDoc = models.CreateMetaDataDoc(datasetDoc.Next, versionDoc)
	} else {
		metaDataDoc = models.CreateMetaDataDoc(datasetDoc.Current, versionDoc)
	}

	bytes, err := json.Marshal(metaDataDoc)
	if err != nil {
		log.ErrorC("fail to marshal metadata resource into bytes", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(bytes)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Debug("get metadata relevant to version", log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
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
		if err == errs.ErrDatasetNotFound || err == errs.ErrEditionNotFound || err == errs.ErrVersionNotFound || err == errs.ErrDimensionNodeNotFound || err == errs.ErrInstanceNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	case "edition":
		if err == errs.ErrDatasetNotFound {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else if err == errs.ErrEditionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	case "version":
		if err == errs.ErrDatasetNotFound {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else if err == errs.ErrEditionNotFound {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else if err == errs.ErrVersionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func setJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}
