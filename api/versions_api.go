package api

import (
	"encoding/json"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

const (
	downloadServiceToken = "X-Download-Service-Token"
	versionDocType       = "version"
)

func (api *DatasetAPI) getVersions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	editionID := vars["edition"]
	logData := log.Data{"dataset_id": id, "edition": editionID}
	auditParams := common.Params{"dataset_id": id, "edition": editionID}

	if err := api.auditor.Record(r.Context(), getVersionsAction, actionAttempted, auditParams); err != nil {
		handleAuditingFailure(w, err, logData)
		return
	}

	authorised, logData := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.ErrorC("failed to find dataset for list of versions", err, logData)
		if err := api.auditor.Record(r.Context(), getVersionsAction, actionUnsuccessful, auditParams); err != nil {
			handleAuditingFailure(w, err, logData)
			return
		}
		handleErrorType(versionDocType, err, w)
		return
	}

	if err := api.dataStore.Backend.CheckEditionExists(id, editionID, state); err != nil {
		log.ErrorC("failed to find edition for list of versions", err, logData)
		if err := api.auditor.Record(r.Context(), getVersionsAction, actionUnsuccessful, auditParams); err != nil {
			handleAuditingFailure(w, err, logData)
			return
		}
		handleErrorType(versionDocType, err, w)
		return
	}

	results, err := api.dataStore.Backend.GetVersions(id, editionID, state)
	if err != nil {
		log.ErrorC("failed to find any versions for dataset edition", err, logData)
		if err := api.auditor.Record(r.Context(), getVersionsAction, actionUnsuccessful, auditParams); err != nil {
			handleAuditingFailure(w, err, logData)
			return
		}
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
		if err := api.auditor.Record(r.Context(), getVersionsAction, actionUnsuccessful, auditParams); err != nil {
			handleAuditingFailure(w, err, logData)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	b, err := json.Marshal(results)
	if err != nil {
		log.ErrorC("failed to marshal list of version resources into bytes", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := api.auditor.Record(r.Context(), getVersionsAction, actionSuccessful, auditParams); err != nil {
		handleAuditingFailure(w, err, logData)
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
	auditParams := common.Params{"dataset_id": id, "edition": editionID, "version": version}

	if err := api.auditor.Record(r.Context(), getVersionAction, actionAttempted, auditParams); err != nil {
		handleAuditingFailure(w, err, logData)
		return
	}

	authorised, logData := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.ErrorC("failed to find dataset", err, logData)
		if err := api.auditor.Record(r.Context(), getVersionAction, actionUnsuccessful, auditParams); err != nil {
			handleAuditingFailure(w, err, logData)
			return
		}
		handleErrorType(versionDocType, err, w)
		return
	}

	if err := api.dataStore.Backend.CheckEditionExists(id, editionID, state); err != nil {
		log.ErrorC("failed to find edition for dataset", err, logData)
		if err := api.auditor.Record(r.Context(), getVersionAction, actionUnsuccessful, auditParams); err != nil {
			handleAuditingFailure(w, err, logData)
			return
		}
		handleErrorType(versionDocType, err, w)
		return
	}

	results, err := api.dataStore.Backend.GetVersion(id, editionID, version, state)
	if err != nil {
		log.ErrorC("failed to find version for dataset edition", err, logData)
		if err := api.auditor.Record(r.Context(), getVersionAction, actionUnsuccessful, auditParams); err != nil {
			handleAuditingFailure(w, err, logData)
			return
		}
		handleErrorType(versionDocType, err, w)
		return
	}

	results.Links.Self.HRef = results.Links.Version.HRef

	if err = models.CheckState("version", results.State); err != nil {
		log.ErrorC("unpublished version has an invalid state", err, log.Data{"state": results.State})
		if err := api.auditor.Record(r.Context(), getVersionAction, actionUnsuccessful, auditParams); err != nil {
			handleAuditingFailure(w, err, logData)
			return
		}
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

	if err := api.auditor.Record(r.Context(), getVersionAction, actionSuccessful, auditParams); err != nil {
		handleAuditingFailure(w, err, logData)
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
