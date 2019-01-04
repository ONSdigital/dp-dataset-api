package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/ONSdigital/go-ns/request"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

var (
	// errors that map to a HTTP 404 response
	notFound = map[error]bool{
		errs.ErrDatasetNotFound: true,
		errs.ErrEditionNotFound: true,
		errs.ErrVersionNotFound: true,
	}

	// errors that map to a HTTP 400 response
	badRequest = map[error]bool{
		errs.ErrUnableToParseJSON:                      true,
		models.ErrPublishedVersionCollectionIDInvalid:  true,
		models.ErrAssociatedVersionCollectionIDInvalid: true,
		models.ErrVersionStateInvalid:                  true,
	}

	// HTTP 500 responses with a specific message
	internalServerErrWithMessage = map[error]bool{
		errs.ErrResourceState: true,
	}
)

// VersionDetails contains the details that uniquely identify a version resource
type VersionDetails struct {
	datasetID string
	edition   string
	version   string
}

func (v VersionDetails) baseAuditParams() common.Params {
	return common.Params{"dataset_id": v.datasetID, "edition": v.edition, "version": v.version}
}

func (api *DatasetAPI) getVersions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	auditParams := common.Params{"dataset_id": datasetID, "edition": edition}
	logData := audit.ToLogData(auditParams)

	if auditErr := api.auditor.Record(ctx, getVersionsAction, audit.Attempted, auditParams); auditErr != nil {
		handleVersionAPIErr(ctx, errs.ErrInternalServer, w, logData)
		return
	}

	b, err := func() ([]byte, error) {
		authorised, logData := api.authenticate(r, logData)

		var state string
		if !authorised {
			state = models.PublishedState
		}

		if err := api.dataStore.Backend.CheckDatasetExists(datasetID, state); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "failed to find dataset for list of versions"), logData)
			return nil, err
		}

		if err := api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "failed to find edition for list of versions"), logData)
			return nil, err
		}

		results, err := api.dataStore.Backend.GetVersions(datasetID, edition, state)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "failed to find any versions for dataset edition"), logData)
			return nil, err
		}

		var hasInvalidState bool
		for _, item := range results.Items {
			if err = models.CheckState("version", item.State); err != nil {
				hasInvalidState = true
				log.ErrorCtx(ctx, errors.WithMessage(err, "unpublished version has an invalid state"), log.Data{"state": item.State})
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
					if item.Downloads.CSVW != nil {
						item.Downloads.CSVW.Private = ""
						item.Downloads.CSVW.Public = ""
					}
				}
			}
		}

		if hasInvalidState {
			return nil, err
		}

		b, err := json.Marshal(results)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "failed to marshal list of version resources into bytes"), logData)
			return nil, err
		}
		return b, nil
	}()

	if err != nil {
		if auditErr := api.auditor.Record(ctx, getVersionsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			err = auditErr
		}
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	if auditErr := api.auditor.Record(ctx, getVersionsAction, audit.Successful, auditParams); auditErr != nil {
		handleVersionAPIErr(ctx, auditErr, w, logData)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "error writing bytes to response"), logData)
		handleVersionAPIErr(ctx, err, w, logData)
	}
	log.InfoCtx(ctx, "getVersions endpoint: request successful", logData)
}

func (api *DatasetAPI) getVersion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]
	auditParams := common.Params{"dataset_id": datasetID, "edition": edition, "version": version}
	logData := audit.ToLogData(auditParams)

	if auditErr := api.auditor.Record(ctx, getVersionAction, audit.Attempted, auditParams); auditErr != nil {
		handleVersionAPIErr(ctx, auditErr, w, logData)
		return
	}

	b, getVersionErr := func() ([]byte, error) {
		authorised, logData := api.authenticate(r, logData)

		var state string
		if !authorised {
			state = models.PublishedState
		}

		if err := api.dataStore.Backend.CheckDatasetExists(datasetID, state); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "failed to find dataset"), logData)
			return nil, err
		}

		if err := api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
			checkEditionErr := errors.WithMessage(err, "failed to find edition for dataset")
			log.ErrorCtx(ctx, checkEditionErr, logData)
			return nil, err
		}

		results, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, state)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "failed to find version for dataset edition"), logData)
			return nil, err
		}

		results.Links.Self.HRef = results.Links.Version.HRef

		if err = models.CheckState("version", results.State); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "unpublished version has an invalid state"), log.Data{"state": results.State})
			return nil, errs.ErrResourceState
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
				if results.Downloads.CSVW != nil {
					results.Downloads.CSVW.Private = ""
					results.Downloads.CSVW.Public = ""
				}
			}
		}

		b, err := json.Marshal(results)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "failed to marshal version resource into bytes"), logData)
			return nil, err
		}
		return b, nil
	}()

	if getVersionErr != nil {
		if auditErr := api.auditor.Record(ctx, getVersionAction, audit.Unsuccessful, auditParams); auditErr != nil {
			getVersionErr = auditErr
		}
		handleVersionAPIErr(ctx, getVersionErr, w, logData)
		return
	}

	if auditErr := api.auditor.Record(ctx, getVersionAction, audit.Successful, auditParams); auditErr != nil {
		handleVersionAPIErr(ctx, auditErr, w, logData)
		return
	}

	setJSONContentType(w)
	_, err := w.Write(b)
	if err != nil {
		log.ErrorCtx(ctx, errors.WithMessage(err, "failed writing bytes to response"), logData)
		handleVersionAPIErr(ctx, err, w, logData)
	}
	log.InfoCtx(ctx, "getVersion endpoint: request successful", logData)
}

func (api *DatasetAPI) putVersion(w http.ResponseWriter, r *http.Request) {

	defer request.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	versionDetails := VersionDetails{
		datasetID: vars["dataset_id"],
		edition:   vars["edition"],
		version:   vars["version"],
	}
	data := log.Data{
		"datasetID": vars["dataset_id"],
		"edition":   vars["edition"],
		"version":   vars["version"],
	}

	currentDataset, currentVersion, versionDoc, err := api.updateVersion(ctx, r.Body, versionDetails)
	if err != nil {
		handleVersionAPIErr(ctx, err, w, data)
		return
	}

	// If update was to add downloads do not try to publish/associate version
	if vars[hasDownloads] != trueStringified {
		if versionDoc.State == models.PublishedState {
			if err := api.publishVersion(ctx, currentDataset, currentVersion, versionDoc, versionDetails); err != nil {
				handleVersionAPIErr(ctx, err, w, data)
				return
			}
		}

		if versionDoc.State == models.AssociatedState && currentVersion.State != models.AssociatedState {
			if err := api.associateVersion(ctx, currentVersion, versionDoc, versionDetails); err != nil {
				handleVersionAPIErr(ctx, err, w, data)
				return
			}
		}
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.InfoCtx(ctx, "putVersion endpoint: request successful", data)
}

func (api *DatasetAPI) updateVersion(ctx context.Context, body io.ReadCloser, versionDetails VersionDetails) (*models.DatasetUpdate, *models.Version, *models.Version, error) {
	ap := versionDetails.baseAuditParams()
	data := audit.ToLogData(ap)

	// attempt to update the version
	currentDataset, currentVersion, versionUpdate, err := func() (*models.DatasetUpdate, *models.Version, *models.Version, error) {
		versionUpdate, err := models.CreateVersion(body)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "putVersion endpoint: failed to model version resource based on request"), data)
			return nil, nil, nil, errs.ErrUnableToParseJSON
		}

		currentDataset, err := api.dataStore.Backend.GetDataset(versionDetails.datasetID)
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "putVersion endpoint: datastore.getDataset returned an error"), data)
			return nil, nil, nil, err
		}

		if err = api.dataStore.Backend.CheckEditionExists(versionDetails.datasetID, versionDetails.edition, ""); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "putVersion endpoint: failed to find edition of dataset"), data)
			return nil, nil, nil, err
		}

		currentVersion, err := api.dataStore.Backend.GetVersion(versionDetails.datasetID, versionDetails.edition, versionDetails.version, "")
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "putVersion endpoint: datastore.GetVersion returned an error"), data)
			return nil, nil, nil, err
		}

		// Combine update version document to existing version document
		populateNewVersionDoc(currentVersion, versionUpdate)
		data["updated_version"] = versionUpdate
		log.InfoCtx(ctx, "putVersion endpoint: combined current version document with update request", data)

		if err = models.ValidateVersion(versionUpdate); err != nil {
			log.ErrorCtx(ctx, errors.Wrap(err, "putVersion endpoint: failed validation check for version update"), nil)
			return nil, nil, nil, err
		}

		if versionUpdate.State == models.PublishedState {
			versionUpdate.CollectionID = ""
		}

		if err := api.dataStore.Backend.UpdateVersion(versionUpdate.ID, versionUpdate); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "putVersion endpoint: failed to update version document"), data)
			return nil, nil, nil, err
		}
		return currentDataset, currentVersion, versionUpdate, nil
	}()

	// audit update unsuccessful if error
	if err != nil {
		if auditErr := api.auditor.Record(ctx, updateVersionAction, audit.Unsuccessful, ap); auditErr != nil {
			audit.LogActionFailure(ctx, updateVersionAction, audit.Unsuccessful, auditErr, data)
		}
		return nil, nil, nil, err
	}

	if auditErr := api.auditor.Record(ctx, updateVersionAction, audit.Successful, ap); auditErr != nil {
		audit.LogActionFailure(ctx, updateVersionAction, audit.Successful, auditErr, data)
	}

	log.InfoCtx(ctx, "update version completed successfully", data)
	return currentDataset, currentVersion, versionUpdate, nil
}

func (api *DatasetAPI) publishVersion(ctx context.Context, currentDataset *models.DatasetUpdate, currentVersion *models.Version, versionDoc *models.Version, versionDetails VersionDetails) error {
	ap := versionDetails.baseAuditParams()
	data := audit.ToLogData(ap)

	if auditErr := api.auditor.Record(ctx, publishVersionAction, audit.Attempted, ap); auditErr != nil {
		audit.LogActionFailure(ctx, publishVersionAction, audit.Attempted, auditErr, data)
		return auditErr
	}

	log.InfoCtx(ctx, "attempting to publish version", data)

	err := func() error {
		editionDoc, err := api.dataStore.Backend.GetEdition(versionDetails.datasetID, versionDetails.edition, "")
		if err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "putVersion endpoint: failed to find the edition we're trying to update"), data)
			return err
		}

		editionDoc.Next.State = models.PublishedState
		if err := editionDoc.PublishLinks(api.host, versionDoc.Links.Version); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "putVersion endpoint: failed to update the edition links for the version we're trying to publish"), data)
			return err
		}

		editionDoc.Current = editionDoc.Next

		if err := api.dataStore.Backend.UpsertEdition(versionDetails.datasetID, versionDetails.edition, editionDoc); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "putVersion endpoint: failed to update edition during publishing"), data)
			return err
		}

		if err := api.dataStore.Backend.SetInstanceIsPublished(ctx, versionDoc.ID); err != nil {
			audit.LogError(ctx, errors.WithMessage(err, "putVersion endpoint: failed to set instance node is_published"), data)
			return err
		}

		// Pass in newVersion variable to include relevant data needed for update on dataset API (e.g. links)
		if err := api.publishDataset(ctx, currentDataset, versionDoc); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "putVersion endpoint: failed to update dataset document once version state changes to publish"), data)
			return err
		}

		// Only want to generate downloads again if there is no public link available
		if currentVersion.Downloads != nil && currentVersion.Downloads.CSV != nil && currentVersion.Downloads.CSV.Public == "" {
			if err := api.downloadGenerator.Generate(versionDetails.datasetID, versionDoc.ID, versionDetails.edition, versionDetails.version); err != nil {
				data["instance_id"] = versionDoc.ID
				data["state"] = versionDoc.State
				log.ErrorCtx(ctx, errors.WithMessage(err, "putVersion endpoint: error while attempting to generate full dataset version downloads on version publish"), data)
				// TODO - TECH DEBT - need to add an error event for this.
				return err
			}
		}

		return nil
	}()

	if err != nil {
		if auditErr := api.auditor.Record(ctx, publishVersionAction, audit.Unsuccessful, ap); auditErr != nil {
			audit.LogActionFailure(ctx, publishVersionAction, audit.Unsuccessful, auditErr, data)
		}
		return err
	}

	if auditErr := api.auditor.Record(ctx, publishVersionAction, audit.Successful, ap); auditErr != nil {
		audit.LogActionFailure(ctx, publishVersionAction, audit.Successful, auditErr, data)
	}

	log.InfoCtx(ctx, "publish version completed successfully", data)
	return nil
}

func (api *DatasetAPI) associateVersion(ctx context.Context, currentVersion *models.Version, versionDoc *models.Version, versionDetails VersionDetails) error {
	ap := versionDetails.baseAuditParams()
	data := audit.ToLogData(ap)

	if auditErr := api.auditor.Record(ctx, associateVersionAction, audit.Attempted, ap); auditErr != nil {
		audit.LogActionFailure(ctx, associateVersionAction, audit.Attempted, auditErr, data)
		return auditErr
	}

	associateVersionErr := func() error {
		if err := api.dataStore.Backend.UpdateDatasetWithAssociation(versionDetails.datasetID, versionDoc.State, versionDoc); err != nil {
			log.ErrorCtx(ctx, errors.WithMessage(err, "putVersion endpoint: failed to update dataset document after a version of a dataset has been associated with a collection"), data)
			return err
		}

		log.InfoCtx(ctx, "putVersion endpoint: generating full dataset version downloads", data)

		if err := api.downloadGenerator.Generate(versionDetails.datasetID, versionDoc.ID, versionDetails.edition, versionDetails.version); err != nil {
			data["instance_id"] = versionDoc.ID
			data["state"] = versionDoc.State
			err = errors.WithMessage(err, "putVersion endpoint: error while attempting to generate full dataset version downloads on version association")
			log.ErrorCtx(ctx, err, data)
			return err
		}
		return nil
	}()

	if associateVersionErr != nil {
		if auditErr := api.auditor.Record(ctx, associateVersionAction, audit.Unsuccessful, ap); auditErr != nil {
			audit.LogActionFailure(ctx, associateVersionAction, audit.Unsuccessful, auditErr, data)
		}
		return associateVersionErr
	}

	if auditErr := api.auditor.Record(ctx, associateVersionAction, audit.Successful, ap); auditErr != nil {
		audit.LogActionFailure(ctx, associateVersionAction, audit.Successful, auditErr, data)
	}

	log.InfoCtx(ctx, "associate version completed successfully", data)
	return associateVersionErr
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

	// TODO - Data Integrity - Updating downloads should be locked down to services
	// with permissions to do so, currently a user could update these fields
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

		if version.Downloads.CSVW == nil {
			if currentVersion.Downloads != nil && currentVersion.Downloads.CSVW != nil {
				version.Downloads.CSVW = currentVersion.Downloads.CSVW
			}
		}
	}

	if version.UsageNotes == nil {
		version.UsageNotes = currentVersion.UsageNotes
	}

	return version
}

func handleVersionAPIErr(ctx context.Context, err error, w http.ResponseWriter, data log.Data) {
	var status int
	switch {
	case notFound[err]:
		status = http.StatusNotFound
	case badRequest[err]:
		status = http.StatusBadRequest
	case internalServerErrWithMessage[err]:
		status = http.StatusInternalServerError
	case strings.HasPrefix(err.Error(), "missing mandatory fields:"):
		status = http.StatusBadRequest
	case strings.HasPrefix(err.Error(), "invalid fields:"):
		status = http.StatusBadRequest
	default:
		err = errs.ErrInternalServer
		status = http.StatusInternalServerError
	}

	if data == nil {
		data = log.Data{}
	}

	log.ErrorCtx(ctx, errors.WithMessage(err, "request unsuccessful"), data)
	http.Error(w, err.Error(), status)
}
