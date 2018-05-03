package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"gopkg.in/mgo.v2/bson"
)

const (
	florenceHeaderKey = "X-Florence-Token"
	authHeaderKey     = "Authorization"

	datasetDocType         = "dataset"
	editionDocType         = "edition"
	versionDocType         = "version"
	downloadServiceToken   = "X-Download-Service-Token"
	dimensionDocType       = "dimension"
	dimensionOptionDocType = "dimension-option"

	// audit actions
	getDatasetsAction = "getDatasets"
	getDatasetAction  = "getDataset"
	getEditionsAction = "getEditions"

	// audit results
	actionAttempted  = "attempted"
	actionSuccessful = "successful"
	notFound         = "notFound"

	auditError = "error while auditing event, failing request"
)

func (api *DatasetAPI) getDatasets(w http.ResponseWriter, r *http.Request) {
	if err := api.auditor.Record(r.Context(), getDatasetsAction, actionAttempted, nil); err != nil {
		log.ErrorC(auditError, err, nil)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	results, err := api.dataStore.Backend.GetDatasets()
	if err != nil {
		log.Error(err, nil)
		api.auditor.Record(r.Context(), getDatasetsAction, notFound, nil)
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

	if err := api.auditor.Record(r.Context(), getDatasetsAction, actionSuccessful, nil); err != nil {
		log.ErrorC(auditError, err, nil)
		http.Error(w, "internal server error", http.StatusInternalServerError)
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

	if err := api.auditor.Record(r.Context(), getDatasetAction, actionAttempted, auditParams); err != nil {
		log.ErrorC("error while auditing getDataset attempt action", err, logData)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	dataset, err := api.dataStore.Backend.GetDataset(id)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id})
		api.auditor.Record(r.Context(), getDatasetAction, notFound, auditParams)
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

	if err := api.auditor.Record(r.Context(), getDatasetAction, actionSuccessful, auditParams); err != nil {
		log.ErrorC("error while auditing getDataset successful action", err, logData)
		w.WriteHeader(http.StatusInternalServerError)
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

func (api *DatasetAPI) getEditions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	logData := log.Data{"dataset_id": id}
	//auditParams := common.Params{"dataset_id": id}

	/*	if err := api.auditor.Record(r.Context(), getEditionsAction, actionAttempted, auditParams); err != nil {
		log.ErrorC(auditError, err, logData)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}*/

	authorised, logData := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	logData["state"] = state
	log.Info("about to check resources exist", logData)

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.ErrorC("unable to find dataset", err, logData)
		handleErrorType(editionDocType, err, w)
		return
	}

	results, err := api.dataStore.Backend.GetEditions(id, state)
	if err != nil {
		log.ErrorC("unable to find editions for dataset", err, logData)
		handleErrorType(editionDocType, err, w)
		return
	}

	var logMessage string
	var b []byte

	if authorised {

		// User has valid authentication to get raw edition document
		b, err = json.Marshal(results)
		if err != nil {
			log.ErrorC("failed to marshal a list of edition resources into bytes", err, logData)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logMessage = "get all editions with auth"

	} else {

		// User is not authenticated and hance has only access to current sub document
		var publicResults []*models.Edition
		for i := range results.Items {
			publicResults = append(publicResults, results.Items[i].Current)
		}

		b, err = json.Marshal(&models.EditionResults{Items: publicResults})
		if err != nil {
			log.ErrorC("failed to marshal a list of public edition resources into bytes", err, logData)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logMessage = "get all editions without auth"
	}

	/*	if err := api.auditor.Record(r.Context(), getEditionsAction, actionSuccessful, auditParams); err != nil {
		log.ErrorC("error while auditing getEditions action successful event", err, logData)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}*/

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug(logMessage, log.Data{"dataset_id": id})
}

func (api *DatasetAPI) getEdition(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	editionID := vars["edition"]
	logData := log.Data{"dataset_id": id, "edition": editionID}

	authorised, logData := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.ErrorC("unable to find dataset", err, logData)
		handleErrorType(editionDocType, err, w)
		return
	}

	edition, err := api.dataStore.Backend.GetEdition(id, editionID, state)
	if err != nil {
		log.ErrorC("unable to find edition", err, logData)
		handleErrorType(editionDocType, err, w)
		return
	}

	var logMessage string
	var b []byte

	if authorised {

		// User has valid authentication to get raw edition document
		b, err = json.Marshal(edition)
		if err != nil {
			log.ErrorC("failed to marshal edition resource into bytes", err, logData)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logMessage = "get edition with auth"

	} else {

		// User is not authenticated and hance has only access to current sub document
		b, err = json.Marshal(edition.Current)
		if err != nil {
			log.ErrorC("failed to marshal public edition resource into bytes", err, logData)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		logMessage = "get public edition without auth"
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug(logMessage, logData)
}

func (api *DatasetAPI) getVersions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	editionID := vars["edition"]
	logData := log.Data{"dataset_id": id, "edition": editionID}

	authorised, logData := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.ErrorC("failed to find dataset for list of versions", err, logData)
		handleErrorType(versionDocType, err, w)
		return
	}

	if err := api.dataStore.Backend.CheckEditionExists(id, editionID, state); err != nil {
		log.ErrorC("failed to find edition for list of versions", err, logData)
		handleErrorType(versionDocType, err, w)
		return
	}

	results, err := api.dataStore.Backend.GetVersions(id, editionID, state)
	if err != nil {
		log.ErrorC("failed to find any versions for dataset edition", err, logData)
		handleErrorType(versionDocType, err, w)
		return
	}

	var hasInvalidState bool
	for _, item := range results.Items {
		if err = models.CheckState("version", item.State); err != nil {
			hasInvalidState = true
			log.ErrorC("unpublished version has an invalid state", err, log.Data{"state": item.State})
		}

		// Only the download service should not have access to the public/private download
		// fields
		if r.Header.Get(downloadServiceToken) != api.downloadServiceToken {
			if item.Downloads != nil {
				if item.Downloads.CSV != nil {
					item.Downloads.CSV.Private = ""
					item.Downloads.CSV.Public = ""
				}
				if item.Downloads.XLS != nil {
					item.Downloads.XLS.Private = ""
					item.Downloads.XLS.Public = ""
				}
			}
		}
	}

	if hasInvalidState {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b, err := json.Marshal(results)
	if err != nil {
		log.ErrorC("failed to marshal list of version resources into bytes", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": id, "edition": editionID})
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug("get all versions", logData)
}

func (api *DatasetAPI) getVersion(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	editionID := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": id, "edition": editionID, "version": version}

	authorised, logData := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.ErrorC("failed to find dataset", err, logData)
		handleErrorType(versionDocType, err, w)
		return
	}

	if err := api.dataStore.Backend.CheckEditionExists(id, editionID, state); err != nil {
		log.ErrorC("failed to find edition for dataset", err, logData)
		handleErrorType(versionDocType, err, w)
		return
	}

	results, err := api.dataStore.Backend.GetVersion(id, editionID, version, state)
	if err != nil {
		log.ErrorC("failed to find version for dataset edition", err, logData)
		handleErrorType(versionDocType, err, w)
		return
	}

	results.Links.Self.HRef = results.Links.Version.HRef

	if err = models.CheckState("version", results.State); err != nil {
		log.ErrorC("unpublished version has an invalid state", err, log.Data{"state": results.State})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Only the download service should not have access to the public/private download
	// fields
	if r.Header.Get(downloadServiceToken) != api.downloadServiceToken {
		if results.Downloads != nil {
			if results.Downloads.CSV != nil {
				results.Downloads.CSV.Private = ""
				results.Downloads.CSV.Public = ""
			}
			if results.Downloads.XLS != nil {
				results.Downloads.XLS.Private = ""
				results.Downloads.XLS.Public = ""
			}
		}
	}

	b, err := json.Marshal(results)
	if err != nil {
		log.ErrorC("failed to marshal version resource into bytes", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Debug("get version", logData)
}

func (api *DatasetAPI) addDataset(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]

	_, err := api.dataStore.Backend.GetDataset(datasetID)
	if err != nil {
		if err != errs.ErrDatasetNotFound {
			log.ErrorC("failed to find dataset", err, log.Data{"dataset_id": datasetID})
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

func (api *DatasetAPI) putVersion(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	edition := vars["edition"]
	version := vars["version"]

	versionDoc, err := models.CreateVersion(r.Body)
	defer r.Body.Close()
	if err != nil {
		log.ErrorC("failed to model version resource based on request", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	currentDataset, err := api.dataStore.Backend.GetDataset(datasetID)
	if err != nil {
		log.ErrorC("failed to find dataset", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	if err = api.dataStore.Backend.CheckEditionExists(datasetID, edition, ""); err != nil {
		log.ErrorC("failed to find edition of dataset", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	currentVersion, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, "")
	if err != nil {
		log.ErrorC("failed to find version of dataset edition", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
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

		editionDoc, err := api.dataStore.Backend.GetEdition(datasetID, edition, "")
		if err != nil {
			log.ErrorC("failed to find the edition we're trying to update", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
			handleErrorType(versionDocType, err, w)
			return
		}

		editionDoc.Next.State = models.PublishedState
		editionDoc.Current = editionDoc.Next

		if err := api.dataStore.Backend.UpsertEdition(datasetID, edition, editionDoc); err != nil {
			log.ErrorC("failed to update edition during publishing", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
			handleErrorType(versionDocType, err, w)
			return
		}

		// Pass in newVersion variable to include relevant data needed for update on dataset API (e.g. links)
		if err := api.publishDataset(currentDataset, newVersion); err != nil {
			log.ErrorC("failed to update dataset document once version state changes to publish", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
			handleErrorType(versionDocType, err, w)
			return
		}

		// Only want to generate downloads again if there is no public link available
		if currentVersion.Downloads != nil && currentVersion.Downloads.CSV != nil && currentVersion.Downloads.CSV.Public == "" {
			if err := api.downloadGenerator.Generate(datasetID, versionDoc.ID, edition, version); err != nil {
				err = errors.Wrap(err, "error while attempting to generate full dataset version downloads on version publish")
				log.Error(err, log.Data{
					"dataset_id":  datasetID,
					"instance_id": versionDoc.ID,
					"edition":     edition,
					"version":     version,
					"state":       versionDoc.State,
				})
				// TODO - TECH DEBT - need to add an error event for this.
				handleErrorType(versionDocType, err, w)
			}
		}
	}

	if versionDoc.State == models.AssociatedState && currentVersion.State != models.AssociatedState {
		if err := api.dataStore.Backend.UpdateDatasetWithAssociation(datasetID, versionDoc.State, versionDoc); err != nil {
			log.ErrorC("failed to update dataset document after a version of a dataset has been associated with a collection", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
			handleErrorType(versionDocType, err, w)
			return
		}

		log.Info("generating full dataset version downloads", log.Data{"dataset_id": datasetID, "edition": edition, "version": version})

		if err := api.downloadGenerator.Generate(datasetID, versionDoc.ID, edition, version); err != nil {
			err = errors.Wrap(err, "error while attempting to generate full dataset version downloads on version association")
			log.Error(err, log.Data{
				"dataset_id":  datasetID,
				"instance_id": versionDoc.ID,
				"edition":     edition,
				"version":     version,
				"state":       versionDoc.State,
			})
			// TODO - TECH DEBT - need to add an error event for this.
			handleErrorType(versionDocType, err, w)
		}
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.Debug("update dataset", log.Data{"dataset_id": datasetID})
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

	if version.Downloads == nil {
		version.Downloads = currentVersion.Downloads
	} else {
		if version.Downloads.XLS == nil {
			if currentVersion.Downloads != nil && currentVersion.Downloads.XLS != nil {
				version.Downloads.XLS = currentVersion.Downloads.XLS
			}
		}

		if version.Downloads.CSV == nil {
			if currentVersion.Downloads != nil && currentVersion.Downloads.CSV != nil {
				version.Downloads.CSV = currentVersion.Downloads.CSV
			}
		}
	}

	return version
}

func (api *DatasetAPI) publishDataset(currentDataset *models.DatasetUpdate, version *models.Version) error {
	if version != nil {
		currentDataset.Next.CollectionID = version.CollectionID

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

func (api *DatasetAPI) getDimensions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	authorised, logData := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, state)
	if err != nil {
		log.ErrorC("failed to get version", err, logData)
		handleErrorType(dimensionDocType, err, w)
		return
	}

	if err = models.CheckState("version", versionDoc.State); err != nil {
		log.ErrorC("unpublished version has an invalid state", err, log.Data{"state": versionDoc.State})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dimensions, err := api.dataStore.Backend.GetDimensions(datasetID, versionDoc.ID)
	if err != nil {
		log.ErrorC("failed to get version dimensions", err, logData)
		handleErrorType(dimensionDocType, err, w)
		return
	}

	results, err := api.createListOfDimensions(versionDoc, dimensions)
	if err != nil {
		log.ErrorC("failed to convert bson to dimension", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	listOfDimensions := &models.DatasetDimensionResults{Items: results}

	b, err := json.Marshal(listOfDimensions)
	if err != nil {
		log.ErrorC("failed to marshal list of dimension resources into bytes", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Debug("get dimensions", log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
}

func (api *DatasetAPI) createListOfDimensions(versionDoc *models.Version, dimensions []bson.M) ([]models.Dimension, error) {

	// Get dimension description from the version document and add to hash map
	dimensionDescriptions := make(map[string]string)
	dimensionLabels := make(map[string]string)
	for _, details := range versionDoc.Dimensions {
		dimensionDescriptions[details.Name] = details.Description
		dimensionLabels[details.Name] = details.Label
	}

	var results []models.Dimension
	for _, dim := range dimensions {
		opt, err := convertBSONToDimensionOption(dim["doc"])
		if err != nil {
			return nil, err
		}

		dimension := models.Dimension{Name: opt.Name}
		dimension.Links.CodeList = opt.Links.CodeList
		dimension.Links.Options = models.LinkObject{ID: opt.Name, HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s/dimensions/%s/options",
			api.host, versionDoc.Links.Dataset.ID, versionDoc.Edition, versionDoc.Links.Version.ID, opt.Name)}
		dimension.Links.Version = *versionDoc.Links.Self

		// Add description to dimension from hash map
		dimension.Description = dimensionDescriptions[dimension.Name]
		dimension.Label = dimensionLabels[dimension.Name]

		results = append(results, dimension)
	}

	return results, nil
}

func convertBSONToDimensionOption(data interface{}) (*models.DimensionOption, error) {
	var dim models.DimensionOption
	b, err := bson.Marshal(data)
	if err != nil {
		return nil, err
	}

	bson.Unmarshal(b, &dim)

	return &dim, nil
}

func (api *DatasetAPI) getDimensionOptions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	editionID := vars["edition"]
	versionID := vars["version"]
	dimension := vars["dimension"]

	logData := log.Data{"dataset_id": datasetID, "edition": editionID, "version": versionID, "dimension": dimension}

	authorised, logData := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	version, err := api.dataStore.Backend.GetVersion(datasetID, editionID, versionID, state)
	if err != nil {
		log.ErrorC("failed to get version", err, logData)
		handleErrorType(versionDocType, err, w)
		return
	}

	if err = models.CheckState("version", version.State); err != nil {
		log.ErrorC("unpublished version has an invalid state", err, log.Data{"state": version.State})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	results, err := api.dataStore.Backend.GetDimensionOptions(version, dimension)
	if err != nil {
		log.ErrorC("failed to get a list of dimension options", err, logData)
		handleErrorType(dimensionOptionDocType, err, w)
		return
	}

	b, err := json.Marshal(results)
	if err != nil {
		log.ErrorC("failed to marshal list of dimension option resources into bytes", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Debug("get dimension options", logData)
}

func (api *DatasetAPI) getMetadata(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, version: version}

	// get dataset document
	datasetDoc, err := api.dataStore.Backend.GetDataset(datasetID)
	if err != nil {
		log.Error(err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	authorised, logData := api.authenticate(r, logData)

	var state string

	// if request is authenticated then access resources of state other than published
	if !authorised {
		// Check for current sub document
		if datasetDoc.Current == nil || datasetDoc.Current.State != models.PublishedState {
			log.ErrorC("found dataset but currently unpublished", errs.ErrDatasetNotFound, log.Data{"dataset_id": datasetID, "edition": edition, "version": version, "dataset": datasetDoc.Current})
			http.Error(w, errs.ErrDatasetNotFound.Error(), http.StatusNotFound)
			return
		}

		state = datasetDoc.Current.State
	}

	if err = api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
		log.ErrorC("failed to find edition for dataset", err, logData)
		handleErrorType(versionDocType, err, w)
		return
	}

	versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, state)
	if err != nil {
		log.ErrorC("failed to find version for dataset edition", err, log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
		handleErrorType(versionDocType, err, w)
		return
	}

	if err = models.CheckState("version", versionDoc.State); err != nil {
		log.ErrorC("unpublished version has an invalid state", err, log.Data{"state": versionDoc.State})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var metaDataDoc *models.Metadata
	// combine version and dataset metadata
	if state != models.PublishedState && versionDoc.CollectionID == datasetDoc.Next.CollectionID {
		metaDataDoc = models.CreateMetaDataDoc(datasetDoc.Next, versionDoc, api.urlBuilder)
	} else {
		metaDataDoc = models.CreateMetaDataDoc(datasetDoc.Current, versionDoc, api.urlBuilder)
	}

	b, err := json.Marshal(metaDataDoc)
	if err != nil {
		log.ErrorC("failed to marshal metadata resource into bytes", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Debug("get metadata relevant to version", logData)
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
			http.Error(w, err.Error(), http.StatusNotFound)
		} else if err == errs.ErrEditionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	case "version":
		if err == errs.ErrDatasetNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else if err == errs.ErrEditionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else if err == errs.ErrVersionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	case "dimension":
		if err == errs.ErrDatasetNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else if err == errs.ErrEditionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else if err == errs.ErrVersionNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else if err == errs.ErrDimensionsNotFound {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// PublishCheck Checks if an version has been published
type PublishCheck struct {
	Datastore store.Storer
}

// Check wraps a HTTP handle. Checks that the state is not published
func (d *PublishCheck) Check(handle func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		vars := mux.Vars(r)
		id := vars["id"]
		edition := vars["edition"]
		version := vars["version"]

		currentVersion, err := d.Datastore.GetVersion(id, edition, version, "")
		if err != nil {
			if err != errs.ErrVersionNotFound {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			// If document cannot be found do not handle error
			handle(w, r)
			return
		}

		if currentVersion != nil {
			if currentVersion.State == models.PublishedState {
				defer func() {
					if err := r.Body.Close(); err != nil {
						log.ErrorC("could not close response body", err, nil)
					}
				}()

				versionDoc, err := models.CreateVersion(r.Body)
				if err != nil {
					log.ErrorC("failed to model version resource based on request", err, log.Data{"dataset_id": id, "edition": edition, "version": version})
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}

				// We can allow public download links to be modified by the exporters when a version is published.
				// Note that a new version will be created which contain only the download information to prevent
				// any forbidden fields from being set on the published version
				if r.Method == "PUT" {
					if versionDoc.Downloads != nil {
						newVersion := new(models.Version)
						if versionDoc.Downloads.CSV != nil && versionDoc.Downloads.CSV.Public != "" {
							newVersion = &models.Version{
								Downloads: &models.DownloadList{
									CSV: &models.DownloadObject{
										Public: versionDoc.Downloads.CSV.Public,
										Size:   versionDoc.Downloads.CSV.Size,
										HRef:   versionDoc.Downloads.CSV.HRef,
									},
								},
							}
						}
						if versionDoc.Downloads.XLS != nil && versionDoc.Downloads.XLS.Public != "" {
							newVersion = &models.Version{
								Downloads: &models.DownloadList{
									XLS: &models.DownloadObject{
										Public: versionDoc.Downloads.XLS.Public,
										Size:   versionDoc.Downloads.XLS.Size,
										HRef:   versionDoc.Downloads.XLS.HRef,
									},
								},
							}
						}
						if newVersion != nil {
							b, err := json.Marshal(newVersion)
							if err != nil {
								http.Error(w, err.Error(), http.StatusForbidden)
								return
							}

							if err := r.Body.Close(); err != nil {
								log.ErrorC("could not close response body", err, nil)
							}
							r.Body = ioutil.NopCloser(bytes.NewBuffer(b))
							handle(w, r)
							return
						}
					}
				}

				err = errors.New("unable to update version as it has been published")
				log.Error(err, log.Data{"version": currentVersion})
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
		}

		handle(w, r)
	})
}

func setJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}

func (api *DatasetAPI) authenticate(r *http.Request, logData map[string]interface{}) (bool, map[string]interface{}) {
	var authorised bool

	if api.EnablePrePublishView {
		var hasCallerIdentity, hasUserIdentity bool

		callerIdentity := common.Caller(r.Context())
		if callerIdentity != "" {
			logData["caller_identity"] = callerIdentity
			hasCallerIdentity = true
		}

		userIdentity := common.User(r.Context())
		if userIdentity != "" {
			logData["user_identity"] = userIdentity
			hasUserIdentity = true
		}

		if hasCallerIdentity || hasUserIdentity {
			authorised = true
		}
		logData["authenticated"] = authorised

		return authorised, logData
	}
	return authorised, logData
}
