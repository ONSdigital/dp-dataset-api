package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

var (
	publishedVersionCollectionIDInvalidErr  = errors.New("Unexpected collection_id in published version")
	associatedVersionCollectionIDInvalidErr = errors.New("Missing collection_id for association between version and a collection")
	versionStateInvalidErr                  = errors.New("Incorrect state, can be one of the following: edition-confirmed, associated or published")
	versionPublishedAction                  = "versionPublished"
	versionDownloadsGenerated               = "versionDownloadsGenerated"
)

func (api *DatasetAPI) getVersions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	editionID := vars["edition"]
	logData := log.Data{"dataset_id": id, "edition": editionID}
	auditParams := common.Params{"dataset_id": id, "edition": editionID}

	if auditErr := api.auditor.Record(r.Context(), getVersionsAction, actionAttempted, auditParams); auditErr != nil {
		handleAuditingFailure(w, auditErr, logData)
		return
	}

	authorised, logData := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.ErrorC("failed to find dataset for list of versions", err, logData)
		if auditErr := api.auditor.Record(r.Context(), getVersionsAction, actionUnsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleErrorType(versionDocType, err, w)
		return
	}

	if err := api.dataStore.Backend.CheckEditionExists(id, editionID, state); err != nil {
		log.ErrorC("failed to find edition for list of versions", err, logData)
		if auditErr := api.auditor.Record(r.Context(), getVersionsAction, actionUnsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleErrorType(versionDocType, err, w)
		return
	}

	results, err := api.dataStore.Backend.GetVersions(id, editionID, state)
	if err != nil {
		log.ErrorC("failed to find any versions for dataset edition", err, logData)
		if auditErr := api.auditor.Record(r.Context(), getVersionsAction, actionUnsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
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

		// Only the download service should have access to the
		// public/private download fields
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
		if auditErr := api.auditor.Record(r.Context(), getVersionsAction, actionUnsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
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

	if auditErr := api.auditor.Record(r.Context(), getVersionsAction, actionSuccessful, auditParams); auditErr != nil {
		handleAuditingFailure(w, auditErr, logData)
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

	if auditErr := api.auditor.Record(r.Context(), getVersionAction, actionAttempted, auditParams); auditErr != nil {
		handleAuditingFailure(w, auditErr, logData)
		return
	}

	authorised, logData := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	if err := api.dataStore.Backend.CheckDatasetExists(id, state); err != nil {
		log.ErrorC("failed to find dataset", err, logData)
		if auditErr := api.auditor.Record(r.Context(), getVersionAction, actionUnsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleErrorType(versionDocType, err, w)
		return
	}

	if err := api.dataStore.Backend.CheckEditionExists(id, editionID, state); err != nil {
		log.ErrorC("failed to find edition for dataset", err, logData)
		if auditErr := api.auditor.Record(r.Context(), getVersionAction, actionUnsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleErrorType(versionDocType, err, w)
		return
	}

	results, err := api.dataStore.Backend.GetVersion(id, editionID, version, state)
	if err != nil {
		log.ErrorC("failed to find version for dataset edition", err, logData)
		if auditErr := api.auditor.Record(r.Context(), getVersionAction, actionUnsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleErrorType(versionDocType, err, w)
		return
	}

	results.Links.Self.HRef = results.Links.Version.HRef

	if err = models.CheckState("version", results.State); err != nil {
		log.ErrorC("unpublished version has an invalid state", err, log.Data{"state": results.State})
		if auditErr := api.auditor.Record(r.Context(), getVersionAction, actionUnsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
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

	if auditErr := api.auditor.Record(r.Context(), getVersionAction, actionSuccessful, auditParams); auditErr != nil {
		handleAuditingFailure(w, auditErr, logData)
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
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["id"]
	edition := vars["edition"]
	version := vars["version"]
	data := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	if err := api.auditor.Record(ctx, putVersionAction, actionAttempted, common.Params{
		"dataset_id": datasetID,
		"edition":    edition,
		"version":    version,
	}); err != nil {
		// TODO
	}
	auditParams := common.Params{"dataset_id": datasetID, "edition": edition, "version": version}

	err := func() error {
		defer r.Body.Close()
		versionDoc, err := models.CreateVersion(r.Body)
		if err != nil {
			logError(ctx, errors.WithMessage(err, "putVersion endpoint: failed to model version resource based on request"), data)
			return errs.ErrVersionBadRequest
		}

		auditParams["requestBody"] = fmt.Sprintf("%+v", versionDoc)

		currentDataset, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			logError(ctx, errors.WithMessage(err, "putVersion endpoint: datastore.getDataset returned an error"), data)
			return err
		}

		if err = api.dataStore.Backend.CheckEditionExists(datasetID, edition, ""); err != nil {
			logError(ctx, errors.WithMessage(err, "putVersion endpoint: failed to find edition of dataset"), data)
			return err
		}

		currentVersion, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, "")
		if err != nil {
			logError(ctx, errors.WithMessage(err, "putVersion endpoint: datastore.GetVersion returned an error"), data)
			return err
		}

		// Combine update version document to existing version document
		populateNewVersionDoc(currentVersion, versionDoc)
		data["updated_version"] = versionDoc
		logInfo(ctx, "putVersion endpoint: combined current version document with update request", data)

		if err = ValidateVersion(versionDoc); err != nil {
			logError(ctx, errors.WithMessage(err, "putVersion endpoint: failed validation check for version update"), nil)
			return err
		}

		if err := api.dataStore.Backend.UpdateVersion(versionDoc.ID, versionDoc); err != nil {
			logError(ctx, errors.WithMessage(err, "putVersion endpoint: failed to update version document"), data)
			return err
		}

		if versionDoc.State == models.PublishedState {

			editionDoc, err := api.dataStore.Backend.GetEdition(datasetID, edition, "")
			if err != nil {
				logError(ctx, errors.WithMessage(err, "putVersion endpoint: failed to find the edition we're trying to update"), data)
				return err
			}

			editionDoc.Next.State = models.PublishedState
			editionDoc.Current = editionDoc.Next

			if err := api.dataStore.Backend.UpsertEdition(datasetID, edition, editionDoc); err != nil {
				logError(ctx, errors.WithMessage(err, "putVersion endpoint: failed to update edition during publishing"), data)
				return err
			}

			auditParams[versionPublishedAction] = actionAttempted
			// Pass in newVersion variable to include relevant data needed for update on dataset API (e.g. links)
			if err := api.publishDataset(currentDataset, versionDoc); err != nil {
				auditParams[versionPublishedAction] = actionUnsuccessful
				logError(ctx, errors.WithMessage(err, "putVersion endpoint: failed to update dataset document once version state changes to publish"), data)
				return err
			}
			auditParams[versionPublishedAction] = actionSuccessful

			// Only want to generate downloads again if there is no public link available
			if currentVersion.Downloads != nil && currentVersion.Downloads.CSV != nil && currentVersion.Downloads.CSV.Public == "" {
				if err := api.downloadGenerator.Generate(datasetID, versionDoc.ID, edition, version); err != nil {
					data["instance_id"] = versionDoc.ID
					data["state"] = versionDoc.State
					logError(ctx, errors.WithMessage(err, "putVersion endpoint: error while attempting to generate full dataset version downloads on version publish"), data)
					// TODO - TECH DEBT - need to add an error event for this.
					return err
				}
			}
		}

		if versionDoc.State == models.AssociatedState && currentVersion.State != models.AssociatedState {
			if err := api.dataStore.Backend.UpdateDatasetWithAssociation(datasetID, versionDoc.State, versionDoc); err != nil {
				logError(ctx, errors.WithMessage(err, "putVersion endpoint: failed to update dataset document after a version of a dataset has been associated with a collection"), data)
				return err
			}

			log.Info("generating full dataset version downloads", data)

			auditParams[versionDownloadsGenerated] = actionAttempted
			if err := api.downloadGenerator.Generate(datasetID, versionDoc.ID, edition, version); err != nil {
				data["instance_id"] = versionDoc.ID
				data["state"] = versionDoc.State
				auditParams[versionDownloadsGenerated] = actionUnsuccessful
				err = errors.WithMessage(err, "putVersion endpoint: error while attempting to generate full dataset version downloads on version association")
				logError(ctx, err, data)
				// TODO - TECH DEBT - need to add an error event for this.
				return err
			}
			auditParams[versionDownloadsGenerated] = actionSuccessful
		}
		return nil
	}()

	if err != nil {
		handleVersionAPIErr(ctx, err, w, data)
	}

	api.auditor.Record(ctx, putVersionAction, actionSuccessful, auditParams)

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	logInfo(ctx, "putVersion endpoint: request successful", data)
}

// ValidateVersion checks the content of the version structure
func ValidateVersion(version *models.Version) error {

	switch version.State {
	case "":
		return errs.ErrVersionMissingState
	case models.EditionConfirmedState:
	case models.PublishedState:
		if version.CollectionID != "" {
			return publishedVersionCollectionIDInvalidErr
		}
	case models.AssociatedState:
		if version.CollectionID == "" {
			return associatedVersionCollectionIDInvalidErr
		}
	default:
		return versionStateInvalidErr
	}

	var missingFields []string
	var invalidFields []string

	if version.ReleaseDate == "" {
		missingFields = append(missingFields, "release_date")
	}

	if version.Downloads != nil {
		if version.Downloads.XLS != nil {
			if version.Downloads.XLS.HRef == "" {
				missingFields = append(missingFields, "Downloads.XLS.HRef")
			}
			if version.Downloads.XLS.Size == "" {
				missingFields = append(missingFields, "Downloads.XLS.Size")
			}
			if _, err := strconv.Atoi(version.Downloads.XLS.Size); err != nil {
				invalidFields = append(invalidFields, "Downloads.XLS.Size not a number")
			}
		}

		if version.Downloads.CSV != nil {
			if version.Downloads.CSV.HRef == "" {
				missingFields = append(missingFields, "Downloads.CSV.HRef")
			}
			if version.Downloads.CSV.Size == "" {
				missingFields = append(missingFields, "Downloads.CSV.Size")
			}
			if _, err := strconv.Atoi(version.Downloads.CSV.Size); err != nil {
				invalidFields = append(invalidFields, "Downloads.CSV.Size not a number")
			}
		}
	}

	if missingFields != nil {
		return fmt.Errorf("missing mandatory fields: %v", missingFields)
	}

	if invalidFields != nil {
		return fmt.Errorf("invalid fields: %v", invalidFields)
	}

	return nil
}

func populateNewVersionDoc(currentVersion *models.Version, version *models.Version) *models.Version {

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
		// will be checked later if state:published
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

	// when changing to (or updating) published state, ensure no CollectionID
	if version.State == models.PublishedState && version.CollectionID != "" {
		version.CollectionID = ""
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

func handleVersionAPIErr(ctx context.Context, err error, w http.ResponseWriter, data log.Data) {
	var status int
	switch {
	case err == errs.ErrDatasetNotFound || err == errs.ErrEditionNotFound || err == errs.ErrVersionNotFound:
		status = http.StatusNotFound
	case err == errs.ErrVersionBadRequest:
		status = http.StatusBadRequest
	case err == publishedVersionCollectionIDInvalidErr:
		status = http.StatusBadRequest
	case err == associatedVersionCollectionIDInvalidErr:
		status = http.StatusBadRequest
	case err == versionStateInvalidErr:
		status = http.StatusBadRequest
	case strings.HasPrefix(err.Error(), "missing mandatory fields:"):
		status = http.StatusBadRequest
	case strings.HasPrefix(err.Error(), "invalid fields:"):
		status = http.StatusBadRequest
	default:
		status = http.StatusInternalServerError
	}

	if data == nil {
		data = log.Data{}
	}

	logError(ctx, errors.WithMessage(err, "request unsuccessful"), data)
	http.Error(w, err.Error(), status)
}
