package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"strconv"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	dphttp "github.com/ONSdigital/dp-net/http"
	dprequest "github.com/ONSdigital/dp-net/request"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

const (
	reqUser   = "req_user"
	reqCaller = "req_caller"
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

func (v VersionDetails) baseLogData() log.Data {
	return log.Data{"dataset_id": v.datasetID, "edition": v.edition, "version": v.version}
}

//getVersions returns a list of versions, the total count of versions that match the query parameters and an error
func (api *DatasetAPI) getVersions(w http.ResponseWriter, r *http.Request, limit, offset int) (interface{}, int, error) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition}
	var err error

	list, totalCount, err := func() ([]models.Version, int, error) {
		authorised := api.authenticate(r, logData)

		var state string
		if !authorised {
			state = models.PublishedState
		}

		if err := api.dataStore.Backend.CheckDatasetExists(datasetID, state); err != nil {
			log.Event(ctx, "failed to find dataset for list of versions", log.ERROR, log.Error(err), logData)
			return nil, 0, err
		}

		if err := api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
			log.Event(ctx, "failed to find edition for list of versions", log.ERROR, log.Error(err), logData)
			return nil, 0, err
		}

		results, totalCount, err := api.dataStore.Backend.GetVersions(ctx, datasetID, edition, state, offset, limit)
		if err != nil {
			log.Event(ctx, "failed to find any versions for dataset edition", log.ERROR, log.Error(err), logData)
			return nil, 0, err
		}

		var hasInvalidState bool
		for _, item := range results {
			if err = models.CheckState("version", item.State); err != nil {
				hasInvalidState = true
				log.Event(ctx, "unpublished version has an invalid state", log.ERROR, log.Error(err), log.Data{"state": item.State})
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
			return nil, 0, err
		}

		return results, totalCount, nil
	}()

	if err != nil {
		handleVersionAPIErr(ctx, err, w, logData)
		return nil, 0, err
	}

	return list, totalCount, nil
}

func (api *DatasetAPI) getVersion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	b, getVersionErr := func() ([]byte, error) {
		authorised := api.authenticate(r, logData)

		versionId, err := checkVersion(ctx, version)
		if err != nil {
			log.Event(ctx, "getVersion endpoint: invalid version", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		var state string
		if !authorised {
			state = models.PublishedState
		}

		if err := api.dataStore.Backend.CheckDatasetExists(datasetID, state); err != nil {
			log.Event(ctx, "failed to find dataset", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		if err := api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
			log.Event(ctx, "failed to find edition for dataset", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		results, err := api.dataStore.Backend.GetVersion(datasetID, edition, versionId, state)
		if err != nil {
			log.Event(ctx, "failed to find version for dataset edition", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		results.Links.Self.HRef = results.Links.Version.HRef

		if err = models.CheckState("version", results.State); err != nil {
			log.Event(ctx, "unpublished version has an invalid state", log.ERROR, log.Error(err), log.Data{"state": results.State})
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
			log.Event(ctx, "failed to marshal version resource into bytes", log.ERROR, log.Error(err), logData)
			return nil, err
		}
		return b, nil
	}()

	if getVersionErr != nil {
		handleVersionAPIErr(ctx, getVersionErr, w, logData)
		return
	}

	setJSONContentType(w)
	_, err := w.Write(b)
	if err != nil {
		log.Event(ctx, "failed writing bytes to response", log.ERROR, log.Error(err), logData)
		handleVersionAPIErr(ctx, err, w, logData)
	}
	log.Event(ctx, "getVersion endpoint: request successful", log.INFO, logData)
}

func (api *DatasetAPI) putVersion(w http.ResponseWriter, r *http.Request) {

	defer dphttp.DrainBody(r)

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
	log.Event(ctx, "putVersion endpoint: request successful", log.INFO, data)
}

func (api *DatasetAPI) detachVersion(w http.ResponseWriter, r *http.Request) {

	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)

	log.Event(ctx, "detachVersion endpoint: endpoint called", log.INFO)

	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]

	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	err := func() error {

		authorised := api.authenticate(r, logData)
		if !authorised {
			log.Event(ctx, "detachVersion endpoint: User is not authorised to detach a dataset version", log.ERROR, log.Error(errs.ErrUnauthorised), logData)
			return errs.ErrNotFound
		}

		versionId, err := checkVersion(ctx, version)
		if err != nil {
			log.Event(ctx, "detachVersion endpoint: invalid version request", log.ERROR, log.Error(err), logData)
			return err
		}

		editionDoc, err := api.dataStore.Backend.GetEdition(datasetID, edition, "")
		if err != nil {
			log.Event(ctx, "detachVersion endpoint: Cannot find the specified edition", log.ERROR, log.Error(errs.ErrEditionNotFound), logData)
			return err
		}

		// Only permit detachment of the latest version.
		if editionDoc.Next.Links.LatestVersion.ID != version {
			log.Event(ctx, "detachVersion endpoint: Detach called againt a version other than latest, aborting", log.ERROR, log.Error(errs.ErrVersionAlreadyExists), logData)
			return errs.ErrVersionAlreadyExists
		}

		// Only permit detachment where state is edition-confirmed or associated
		state := editionDoc.Next.State
		if state != models.AssociatedState && state != models.EditionConfirmedState {
			log.Event(ctx, "detachVersion endpoint: You can only detach a version with a state of edition-confirmed or associated", log.ERROR, log.Error(errs.ErrIncorrectStateToDetach), logData)
			return errs.ErrIncorrectStateToDetach
		}

		versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, versionId, editionDoc.Next.State)
		if err != nil {
			log.Event(ctx, "detachVersion endpoint: Cannot find the specified version", log.ERROR, log.Error(errs.ErrVersionNotFound), logData)
			return errs.ErrVersionNotFound
		}

		datasetDoc, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			log.Event(ctx, "detachVersion endpoint: datastore.GetDatasets returned an error", log.ERROR, log.Error(err), logData)
			return err
		}

		// Detach the version
		versionDoc.State = models.DetachedState
		if err = api.dataStore.Backend.UpdateVersion(versionDoc.ID, versionDoc); err != nil {
			log.Event(ctx, "detachVersion endpoint: failed to update version document", log.ERROR, log.Error(err), logData)
			return err
		}

		// Only rollback dataset & edition if there's a "Current" sub-document to roll back to (i.e if a version has been published).
		if datasetDoc.Current != nil {
			// Rollback the edition
			editionDoc.Next = editionDoc.Current
			if err = api.dataStore.Backend.UpsertEdition(datasetID, edition, editionDoc); err != nil {
				log.Event(ctx, "detachVersion endpoint: failed to update edition document", log.ERROR, log.Error(err), logData)
				return err
			}

			// Rollback the dataset
			datasetDoc.Next = datasetDoc.Current
			if err = api.dataStore.Backend.UpsertDataset(datasetID, datasetDoc); err != nil {
				log.Event(ctx, "detachVersion endpoint: failed to update dataset document", log.ERROR, log.Error(err), logData)
				return err
			}
		}

		return nil
	}()

	if err != nil {
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	w.WriteHeader(http.StatusOK)
	log.Event(ctx, "detachVersion endpoint: request successful", log.INFO, logData)
}

func (api *DatasetAPI) updateVersion(ctx context.Context, body io.ReadCloser, versionDetails VersionDetails) (*models.DatasetUpdate, *models.Version, *models.Version, error) {
	data := versionDetails.baseLogData()

	// attempt to update the version
	currentDataset, currentVersion, versionUpdate, err := func() (*models.DatasetUpdate, *models.Version, *models.Version, error) {

		version, err := checkVersion(ctx, versionDetails.version)
		if err != nil {
			log.Event(ctx, "putVersion endpoint: invalid version request", log.ERROR, log.Error(err), data)
			return nil, nil, nil, err
		}

		versionUpdate, err := models.CreateVersion(body, versionDetails.datasetID)
		if err != nil {
			log.Event(ctx, "putVersion endpoint: failed to model version resource based on request", log.ERROR, log.Error(err), data)
			return nil, nil, nil, errs.ErrUnableToParseJSON
		}

		currentDataset, err := api.dataStore.Backend.GetDataset(versionDetails.datasetID)
		if err != nil {
			log.Event(ctx, "putVersion endpoint: datastore.getDataset returned an error", log.ERROR, log.Error(err), data)
			return nil, nil, nil, err
		}

		if err = api.dataStore.Backend.CheckEditionExists(versionDetails.datasetID, versionDetails.edition, ""); err != nil {
			log.Event(ctx, "putVersion endpoint: failed to find edition of dataset", log.ERROR, log.Error(err), data)
			return nil, nil, nil, err
		}

		currentVersion, err := api.dataStore.Backend.GetVersion(versionDetails.datasetID, versionDetails.edition, version, "")
		if err != nil {
			log.Event(ctx, "putVersion endpoint: datastore.GetVersion returned an error", log.ERROR, log.Error(err), data)
			return nil, nil, nil, err
		}

		// Combine update version document to existing version document
		populateNewVersionDoc(currentVersion, versionUpdate)
		data["updated_version"] = versionUpdate
		log.Event(ctx, "putVersion endpoint: combined current version document with update request", log.INFO, data)

		if err = models.ValidateVersion(versionUpdate); err != nil {
			log.Event(ctx, "putVersion endpoint: failed validation check for version update", log.ERROR, log.Error(err))
			return nil, nil, nil, err
		}

		if err := api.dataStore.Backend.UpdateVersion(versionUpdate.ID, versionUpdate); err != nil {
			log.Event(ctx, "putVersion endpoint: failed to update version document", log.ERROR, log.Error(err), data)
			return nil, nil, nil, err
		}
		return currentDataset, currentVersion, versionUpdate, nil
	}()

	// audit update unsuccessful if error
	if err != nil {
		return nil, nil, nil, err
	}

	log.Event(ctx, "update version completed successfully", log.INFO, data)
	return currentDataset, currentVersion, versionUpdate, nil
}

func (api *DatasetAPI) publishVersion(ctx context.Context, currentDataset *models.DatasetUpdate, currentVersion *models.Version, versionDoc *models.Version, versionDetails VersionDetails) error {
	data := versionDetails.baseLogData()
	log.Event(ctx, "attempting to publish version", log.INFO, data)
	err := func() error {
		editionDoc, err := api.dataStore.Backend.GetEdition(versionDetails.datasetID, versionDetails.edition, "")
		if err != nil {
			log.Event(ctx, "putVersion endpoint: failed to find the edition we're trying to update", log.ERROR, log.Error(err), data)
			return err
		}

		editionDoc.Next.State = models.PublishedState
		if err := editionDoc.PublishLinks(ctx, api.host, versionDoc.Links.Version); err != nil {
			log.Event(ctx, "putVersion endpoint: failed to update the edition links for the version we're trying to publish", log.ERROR, log.Error(err), data)
			return err
		}

		editionDoc.Current = editionDoc.Next

		if err := api.dataStore.Backend.UpsertEdition(versionDetails.datasetID, versionDetails.edition, editionDoc); err != nil {
			log.Event(ctx, "putVersion endpoint: failed to update edition during publishing", log.ERROR, log.Error(err), data)
			return err
		}

		if err := api.dataStore.Backend.SetInstanceIsPublished(ctx, versionDoc.ID); err != nil {
			if user := dprequest.User(ctx); user != "" {
				data[reqUser] = user
			}
			if caller := dprequest.Caller(ctx); caller != "" {
				data[reqCaller] = caller
			}
			err := errors.WithMessage(err, "putVersion endpoint: failed to set instance node is_published")
			log.Event(ctx, "failed to publish instance version", log.ERROR, log.Error(err), data)
			return err
		}

		// Pass in newVersion variable to include relevant data needed for update on dataset API (e.g. links)
		if err := api.publishDataset(ctx, currentDataset, versionDoc); err != nil {
			log.Event(ctx, "putVersion endpoint: failed to update dataset document once version state changes to publish", log.ERROR, log.Error(err), data)
			return err
		}

		// Only want to generate downloads again if there is no public link available
		if currentVersion.Downloads != nil && currentVersion.Downloads.CSV != nil && currentVersion.Downloads.CSV.Public == "" {
			if err := api.downloadGenerator.Generate(ctx, versionDetails.datasetID, versionDoc.ID, versionDetails.edition, versionDetails.version); err != nil {
				data["instance_id"] = versionDoc.ID
				data["state"] = versionDoc.State
				log.Event(ctx, "putVersion endpoint: error while attempting to generate full dataset version downloads on version publish", log.ERROR, log.Error(err), data)
				// TODO - TECH DEBT - need to add an error event for this.
				return err
			}
		}

		return nil
	}()

	if err != nil {
		return err
	}

	log.Event(ctx, "publish version completed successfully", log.INFO, data)
	return nil
}

func (api *DatasetAPI) associateVersion(ctx context.Context, currentVersion *models.Version, versionDoc *models.Version, versionDetails VersionDetails) error {
	data := versionDetails.baseLogData()

	associateVersionErr := func() error {
		if err := api.dataStore.Backend.UpdateDatasetWithAssociation(versionDetails.datasetID, versionDoc.State, versionDoc); err != nil {
			log.Event(ctx, "putVersion endpoint: failed to update dataset document after a version of a dataset has been associated with a collection", log.ERROR, log.Error(err), data)
			return err
		}

		log.Event(ctx, "putVersion endpoint: generating full dataset version downloads", log.INFO, data)

		if err := api.downloadGenerator.Generate(ctx, versionDetails.datasetID, versionDoc.ID, versionDetails.edition, versionDetails.version); err != nil {
			data["instance_id"] = versionDoc.ID
			data["state"] = versionDoc.State
			log.Event(ctx, "putVersion endpoint: error while attempting to generate full dataset version downloads on version association", log.ERROR, log.Error(err), data)
			return err
		}
		return nil
	}()

	if associateVersionErr != nil {
		return associateVersionErr
	}

	log.Event(ctx, "associate version completed successfully", log.INFO, data)
	return associateVersionErr
}

func populateNewVersionDoc(currentVersion *models.Version, version *models.Version) *models.Version {

	var alerts []models.Alert

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
	case strings.HasPrefix(err.Error(), "invalid version requested"):
		status = http.StatusBadRequest
	default:
		err = errs.ErrInternalServer
		status = http.StatusInternalServerError
	}

	if data == nil {
		data = log.Data{}
	}

	log.Event(ctx, "request unsuccessful", log.ERROR, log.Error(err), data)
	http.Error(w, err.Error(), status)
}

func checkVersion(ctx context.Context, version string) (int, error) {
	versionId, err := strconv.Atoi(version)
	if !(versionId > 0) {
		log.Event(ctx, "version is not a positive integer", log.ERROR, log.Error(errs.ErrInvalidVersion), log.Data{"version": version})
		return versionId, errs.ErrInvalidVersion
	}
	if err != nil {
		log.Event(ctx, "invalid version provided", log.ERROR, log.Error(err), log.Data{"version": version})
		return versionId, errs.ErrInvalidVersion
	}
	return versionId, nil
}
