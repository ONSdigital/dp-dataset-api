package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"

	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/utils"
	dpresponse "github.com/ONSdigital/dp-net/v3/handlers/response"
	dphttp "github.com/ONSdigital/dp-net/v3/http"
	"github.com/ONSdigital/dp-net/v3/links"
	dprequest "github.com/ONSdigital/dp-net/v3/request"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/jinzhu/copier"
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
		errs.ErrInvalidBody:                            true,
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

// getVersions returns a list of versions, the total count of versions that match the query parameters and an error
// TODO: Refactor this to reduce the complexity
//
//nolint:gocyclo,gocognit,gocritic // high cyclomactic & cognitive complexity not in scope for maintenance. Named results requires similar levels of refactoring.
func (api *DatasetAPI) getVersions(w http.ResponseWriter, r *http.Request, limit, offset int) (interface{}, int, error) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition}
	var err error

	list, totalCount, err := func() ([]models.Version, int, error) {
		authorised := api.authenticate(r, logData)
		var results []models.Version
		var totalCount int
		var state string
		if !authorised {
			state = models.PublishedState
		}

		if err := api.dataStore.Backend.CheckDatasetExists(ctx, datasetID, state); err != nil {
			log.Error(ctx, "failed to find dataset for list of versions", err, logData)
			return nil, 0, err
		}

		// Check if dataset exists
		dataset, err := api.dataStore.Backend.GetDataset(ctx, datasetID)
		if err != nil {
			log.Error(ctx, "failed to retrieve dataset details", err, logData)
			return nil, 0, err
		}

		datasetType := dataset.Next.Type

		// Check if edition exists based on dataset type
		if datasetType == models.Static.String() {
			err = api.dataStore.Backend.CheckEditionExistsStatic(ctx, datasetID, edition, state)
		} else {
			err = api.dataStore.Backend.CheckEditionExists(ctx, datasetID, edition, state)
		}

		if err != nil {
			log.Error(ctx, "failed to verify edition existence for dataset", err, logData)
			return nil, 0, err
		}

		// Retrieve versions based on dataset type
		if datasetType == models.Static.String() {
			results, totalCount, err = api.dataStore.Backend.GetVersionsStatic(ctx, datasetID, edition, state, offset, limit)
		} else {
			results, totalCount, err = api.dataStore.Backend.GetVersions(ctx, datasetID, edition, state, offset, limit)
		}

		if err != nil {
			log.Error(ctx, "failed to retrieve versions for dataset edition", err, logData)
			return nil, 0, err
		}

		var hasInvalidState bool
		for i := range results {
			item := &results[i]
			if err = models.CheckState("version", item.State); err != nil {
				hasInvalidState = true
				log.Error(ctx, "unpublished version has an invalid state", err, log.Data{"state": item.State})
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

	if api.enableURLRewriting {
		datasetLinksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetDatasetAPIURL())
		codeListLinksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetCodeListAPIURL())

		list, err = utils.RewriteVersions(ctx, list, datasetLinksBuilder, codeListLinksBuilder, api.urlBuilder.GetDownloadServiceURL())
		if err != nil {
			log.Error(ctx, "getVersions endpoint: error rewriting dimension, version, download or distribution links", err)
			handleVersionAPIErr(ctx, err, w, logData)
			return nil, 0, err
		}
	}

	return list, totalCount, nil
}

// TODO: Refactor this to reduce the complexity
//
//nolint:gocyclo,gocognit // high cyclomactic & cognitive complexity not in scope for maintenance
func (api *DatasetAPI) getVersion(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	versionNumber := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": versionNumber}

	v, getVersionErr := func() (*models.Version, error) {
		authorised := api.authenticate(r, logData)

		versionID, err := models.ParseAndValidateVersionNumber(ctx, versionNumber)
		if err != nil {
			log.Error(ctx, "getVersion endpoint: invalid version", err, logData)
			return nil, err
		}

		var state string
		if !authorised {
			state = models.PublishedState
		}

		if err := api.dataStore.Backend.CheckDatasetExists(ctx, datasetID, state); err != nil {
			log.Error(ctx, "failed to find dataset", err, logData)
			return nil, err
		}

		// get dataset if dataset exists
		dataset, err := api.dataStore.Backend.GetDataset(ctx, datasetID)
		if err != nil {
			log.Error(ctx, "failed to retrieve dataset details", err, logData)
			return nil, err
		}

		datasetType := dataset.Next.Type
		// Check if edition exists based on dataset type
		if datasetType == models.Static.String() {
			err = api.dataStore.Backend.CheckEditionExistsStatic(ctx, datasetID, edition, state)
		} else {
			err = api.dataStore.Backend.CheckEditionExists(ctx, datasetID, edition, state)
		}

		if err != nil {
			log.Error(ctx, "failed to verify edition existence for dataset", err, logData)
			return nil, err
		}

		version := &models.Version{}
		// Retrieve versions based on dataset type
		if datasetType == models.Static.String() {
			version, err = api.dataStore.Backend.GetVersionStatic(ctx, datasetID, edition, versionID, state)
		} else {
			version, err = api.dataStore.Backend.GetVersion(ctx, datasetID, edition, versionID, state)
		}

		if err != nil {
			log.Error(ctx, "failed to find version for dataset edition", err, logData)
			return nil, err
		}

		version.Links.Self.HRef = version.Links.Version.HRef

		if err = models.CheckState("version", version.State); err != nil {
			log.Error(ctx, "unpublished version has an invalid state", err, log.Data{"state": version.State})
			return nil, errs.ErrResourceState
		}

		// Only the download service should not have access to the public/private download
		// fields
		if r.Header.Get(downloadServiceToken) != api.downloadServiceToken {
			if version.Downloads != nil {
				if version.Downloads.CSV != nil {
					version.Downloads.CSV.Private = ""
					version.Downloads.CSV.Public = ""
				}
				if version.Downloads.XLS != nil {
					version.Downloads.XLS.Private = ""
					version.Downloads.XLS.Public = ""
				}
				if version.Downloads.CSVW != nil {
					version.Downloads.CSVW.Private = ""
					version.Downloads.CSVW.Public = ""
				}
			}
		}
		return version, nil
	}()

	if getVersionErr != nil {
		handleVersionAPIErr(ctx, getVersionErr, w, logData)
		return
	}

	if api.enableURLRewriting {
		datasetLinksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetDatasetAPIURL())
		codeListLinksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetCodeListAPIURL())

		var err error

		err = utils.RewriteVersionLinks(ctx, v.Links, datasetLinksBuilder)
		if err != nil {
			log.Error(ctx, "getVersion endpoint: failed to rewrite version links", err)
			handleVersionAPIErr(ctx, err, w, logData)
			return
		}

		v.Dimensions, err = utils.RewriteDimensions(ctx, v.Dimensions, datasetLinksBuilder, codeListLinksBuilder)
		if err != nil {
			log.Error(ctx, "getVersion endpoint: failed to rewrite dimensions", err)
			handleVersionAPIErr(ctx, err, w, logData)
			return
		}

		err = utils.RewriteDownloadLinks(ctx, v.Downloads, api.urlBuilder.GetDownloadServiceURL())
		if err != nil {
			log.Error(ctx, "getVersion endpoint: failed to rewrite download links", err)
			handleVersionAPIErr(ctx, err, w, logData)
			return
		}

		v.Distributions, err = utils.RewriteDistributions(ctx, v.Distributions, api.urlBuilder.GetDownloadServiceURL())
		if err != nil {
			log.Error(ctx, "getVersion endpoint: failed to rewrite distributions DownloadURLs", err)
			handleVersionAPIErr(ctx, err, w, logData)
			return
		}
	}

	setJSONContentType(w)
	if v.ETag != "" {
		dpresponse.SetETag(w, v.ETag)
	}

	versionBytes, err := json.Marshal(v)
	if err != nil {
		log.Error(ctx, "failed to marshal version resource into bytes", err, logData)
		handleVersionAPIErr(ctx, err, w, logData)
	}

	_, err = w.Write(versionBytes)
	if err != nil {
		log.Error(ctx, "failed writing bytes to response", err, logData)
		handleVersionAPIErr(ctx, err, w, logData)
	}
	log.Info(ctx, "getVersion endpoint: request successful", logData)
}

func (api *DatasetAPI) putVersion(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)

	data := log.Data{
		"datasetID": vars["dataset_id"],
		"edition":   vars["edition"],
		"version":   vars["version"],
	}

	if api.EnableStateMachine {
		version, err := models.CreateVersion(r.Body, vars["dataset_id"])
		if err != nil {
			handleVersionAPIErr(ctx, err, w, data)
			return
		}

		err = api.smDatasetAPI.AmendVersion(r.Context(), vars, version)
		if err != nil {
			handleVersionAPIErr(ctx, err, w, data)
			return
		}
	} else {
		versionDetails := VersionDetails{
			datasetID: vars["dataset_id"],
			edition:   vars["edition"],
			version:   vars["version"],
		}

		currentDataset, currentVersion, versionUpdate, err := api.updateVersion(ctx, r.Body, versionDetails)
		if err != nil {
			handleVersionAPIErr(ctx, err, w, data)
			return
		}

		// If update was to add downloads do not try to publish/associate version
		if vars[hasDownloads] != trueStringified {
			data["updated_state"] = versionUpdate.State
			if versionUpdate.State == models.PublishedState {
				if err := api.publishVersion(ctx, currentDataset, currentVersion, versionUpdate, versionDetails); err != nil {
					handleVersionAPIErr(ctx, err, w, data)
					return
				}
			}

			if versionUpdate.State == models.AssociatedState && currentVersion.State != models.AssociatedState {
				if err := api.associateVersion(ctx, currentVersion, versionUpdate, versionDetails); err != nil {
					handleVersionAPIErr(ctx, err, w, data)
					return
				}
			}
		}
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.Info(ctx, "putVersion endpoint: request successful", data)
}

func (api *DatasetAPI) associateVersion(ctx context.Context, currentVersion, versionDoc *models.Version, versionDetails VersionDetails) error {
	data := versionDetails.baseLogData()
	data["type"] = currentVersion.Type
	data["version_update"] = versionDoc
	log.Info(ctx, "putVersion endpoint: associated version", data)

	associateVersionErr := func() error {
		if err := api.dataStore.Backend.UpdateDatasetWithAssociation(ctx, versionDetails.datasetID, versionDoc.State, versionDoc); err != nil {
			log.Error(ctx, "putVersion endpoint: failed to update dataset document after a version of a dataset has been associated with a collection", err, data)
			return err
		}

		// Get the download generator from the map, depending of the Version document type
		t, err := models.GetDatasetType(currentVersion.Type)
		if err != nil {
			return fmt.Errorf("error getting type of version: %w", err)
		}
		generator, ok := api.downloadGenerators[t]
		if !ok {
			return fmt.Errorf("no downloader available for type %s", t.String())
		}

		if err := generator.Generate(ctx, versionDetails.datasetID, versionDoc.ID, versionDetails.edition, versionDetails.version); err != nil {
			data["instance_id"] = versionDoc.ID
			data["state"] = versionDoc.State
			log.Error(ctx, "putVersion endpoint: error while attempting to generate full dataset version downloads on version association", err, data)
			return err
		}
		data["type"] = t.String()
		log.Info(ctx, "putVersion endpoint (associateVersion): generated full dataset version downloads", data)
		return nil
	}()

	if associateVersionErr != nil {
		return associateVersionErr
	}

	log.Info(ctx, "associate version completed successfully", data)
	return associateVersionErr
}

// TODO: Refactor this to reduce the complexity
//
//nolint:gocyclo,gocognit // high cyclomactic & cognitive complexity not in scope for maintenance
func (api *DatasetAPI) detachVersion(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)

	log.Info(ctx, "detachVersion endpoint: endpoint called")

	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]

	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	if err := func() error {
		authorised := api.authenticate(r, logData)
		if !authorised {
			log.Error(ctx, "detachVersion endpoint: User is not authorised to detach a dataset version", errs.ErrUnauthorised, logData)
			return errs.ErrNotFound
		}

		versionID, err := models.ParseAndValidateVersionNumber(ctx, version)
		if err != nil {
			log.Error(ctx, "detachVersion endpoint: invalid version request", err, logData)
			return err
		}

		editionDoc, err := api.dataStore.Backend.GetEdition(ctx, datasetID, edition, "")
		if err != nil {
			log.Error(ctx, "detachVersion endpoint: Cannot find the specified edition", errs.ErrEditionNotFound, logData)
			return err
		}

		// Only permit detachment of the latest version.
		if editionDoc.Next.Links.LatestVersion.ID != version {
			log.Error(ctx, "detachVersion endpoint: Detach called againt a version other than latest, aborting", errs.ErrVersionAlreadyExists, logData)
			return errs.ErrVersionAlreadyExists
		}

		// Only permit detachment where state is edition-confirmed or associated
		state := editionDoc.Next.State
		if state != models.AssociatedState && state != models.EditionConfirmedState {
			log.Error(ctx, "detachVersion endpoint: You can only detach a version with a state of edition-confirmed or associated", errs.ErrIncorrectStateToDetach, logData)
			return errs.ErrIncorrectStateToDetach
		}

		versionDoc, err := api.dataStore.Backend.GetVersion(ctx, datasetID, edition, versionID, editionDoc.Next.State)
		if err != nil {
			log.Error(ctx, "detachVersion endpoint: Cannot find the specified version", errs.ErrVersionNotFound, logData)
			return errs.ErrVersionNotFound
		}

		datasetDoc, err := api.dataStore.Backend.GetDataset(ctx, datasetID)
		if err != nil {
			log.Error(ctx, "detachVersion endpoint: datastore.GetDatasets returned an error", err, logData)
			return err
		}

		// Detach the version
		update := &models.Version{
			State: models.DetachedState,
		}
		logData["updated_state"] = update.State
		if _, err = api.dataStore.Backend.UpdateVersion(ctx, versionDoc, update, headers.IfMatchAnyETag); err != nil {
			log.Error(ctx, "detachVersion endpoint: failed to update version document", err, logData)
			return err
		}

		// Only rollback dataset & edition if there's a "Current" sub-document to roll back to (i.e if a version has been published).
		if datasetDoc.Current != nil {
			// Rollback the edition
			editionDoc.Next = editionDoc.Current
			if err = api.dataStore.Backend.UpsertEdition(ctx, datasetID, edition, editionDoc); err != nil {
				log.Error(ctx, "detachVersion endpoint: failed to update edition document", err, logData)
				return err
			}

			// Rollback the dataset
			datasetDoc.Next = datasetDoc.Current
			if err = api.dataStore.Backend.UpsertDataset(ctx, datasetID, datasetDoc); err != nil {
				log.Error(ctx, "detachVersion endpoint: failed to update dataset document", err, logData)
				return err
			}
		} else {
			// For first (unpublished) versions:
			// delete edition doc
			if err := api.dataStore.Backend.DeleteEdition(ctx, editionDoc.ID); err != nil {
				log.Error(ctx, "detachVersion endpoint: failed to delete edition document", err, logData)
				return err
			}

			// remove edition and version links from datasetDoc
			if err := api.dataStore.Backend.RemoveDatasetVersionAndEditionLinks(ctx, datasetID); err != nil {
				log.Error(ctx, "detachVersion endpoint: failed to update dataset document", err, logData)
				return err
			}
		}

		return nil
	}(); err != nil {
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	w.WriteHeader(http.StatusOK)
	log.Info(ctx, "detachVersion endpoint: request successful", logData)
}

//nolint:gocognit,gocyclo // // high cyclomactic & cognitive complexity
func (api *DatasetAPI) updateVersion(ctx context.Context, body io.ReadCloser, versionDetails VersionDetails) (currentDataset *models.DatasetUpdate, currentVersion, combinedVersionUpdate *models.Version, err error) {
	data := versionDetails.baseLogData()

	reqID := ctx.Value(dprequest.RequestIdKey) // used to differentiate logs of concurrent calls to this function from different services

	versionNumber, err := models.ParseAndValidateVersionNumber(ctx, versionDetails.version)
	if err != nil {
		log.Error(ctx, "putVersion endpoint: invalid version request", err, data)
		return nil, nil, nil, err
	}

	// reads http header and creates struct for new versionNumber
	versionUpdate, err := models.CreateVersion(body, versionDetails.datasetID)
	if err != nil {
		log.Error(ctx, "putVersion endpoint: failed to model version resource based on request", err, data)
		return nil, nil, nil, errs.ErrUnableToParseJSON
	}

	currentDataset, err = api.dataStore.Backend.GetDataset(ctx, versionDetails.datasetID)
	if err != nil {
		log.Error(ctx, "putVersion endpoint: datastore.getDataset returned an error", err, data)
		return nil, nil, nil, err
	}

	if versionUpdate != nil {
		if versionUpdate.Type == models.Static.String() {
			if err = api.dataStore.Backend.CheckEditionExistsStatic(ctx, versionDetails.datasetID, versionDetails.edition, ""); err != nil {
				log.Error(ctx, "putVersion endpoint: failed to find version of dataset", err, data)
				return nil, nil, nil, err
			}
		} else {
			if err = api.dataStore.Backend.CheckEditionExists(ctx, versionDetails.datasetID, versionDetails.edition, ""); err != nil {
				log.Error(ctx, "putVersion endpoint: failed to find edition of dataset", err, data)
				return nil, nil, nil, err
			}
		}
	}

	if versionUpdate != nil {
		if versionUpdate.Type == models.Static.String() {
			currentVersion, err = api.dataStore.Backend.GetVersionStatic(ctx, versionDetails.datasetID, versionDetails.edition, versionNumber, "")
			if err != nil {
				log.Error(ctx, "putVersion endpoint: datastore.GetVersionStatic returned an error", err, data)
				return nil, nil, nil, err
			}
		} else {
			currentVersion, err = api.dataStore.Backend.GetVersion(ctx, versionDetails.datasetID, versionDetails.edition, versionNumber, "")
			if err != nil {
				log.Error(ctx, "putVersion endpoint: datastore.GetVersion returned an error", err, data)
				return nil, nil, nil, err
			}
		}
	}

	// doUpdate is an aux function that combines the existing version document with the update received in the body request,
	// then it validates the new model, and performs the update in MongoDB, passing the existing model ETag (if it exists) to be used in the query selector
	// Note that the combined version update does not mutate versionUpdate because multiple retries might generate a different value depending on the currentVersion at that point.
	var doUpdate = func() error {
		combinedVersionUpdate, err = populateNewVersionDoc(currentVersion, versionUpdate)
		if err != nil {
			return err
		}

		data["updated_version"] = combinedVersionUpdate

		if err = models.ValidateVersion(combinedVersionUpdate); err != nil {
			log.Error(ctx, "putVersion endpoint: failed validation check for version update", err)
			return err
		}

		eTag := headers.IfMatchAnyETag
		if currentVersion.ETag != "" {
			eTag = currentVersion.ETag
		}

		if versionUpdate != nil {
			if versionUpdate.Type == models.Static.String() {
				if _, err := api.dataStore.Backend.UpdateVersionStatic(ctx, currentVersion, combinedVersionUpdate, eTag); err != nil {
					log.Error(ctx, "putVersion endpoint: UpdateVersionStatic returned an error", err, data)
					return err
				}
			} else {
				if _, err := api.dataStore.Backend.UpdateVersion(ctx, currentVersion, combinedVersionUpdate, eTag); err != nil {
					log.Error(ctx, "putVersion endpoint: UpdateVersion returned an error", err, data)
					return err
				}
			}
		}

		return nil
	}

	if versionUpdate != nil {
		if versionUpdate.Type == models.Static.String() {
			// acquire versions lock to prevent race conditions on versions collection
			lockID, err := api.dataStore.Backend.AcquireVersionsLock(ctx, currentVersion.ID)
			if err != nil {
				return nil, nil, nil, err
			}
			defer func() {
				api.dataStore.Backend.UnlockVersions(ctx, lockID)
			}()
		} else {
			// acquire instance lock to prevent race conditions on instance collection
			lockID, err := api.dataStore.Backend.AcquireInstanceLock(ctx, currentVersion.ID)
			if err != nil {
				return nil, nil, nil, err
			}
			defer func() {
				api.dataStore.Backend.UnlockInstance(ctx, lockID)
			}()
		}
	}

	// Try to perform the update. If there was a race condition and another caller performed the update
	// before we could acquire the lock, this will result in the ETag being changed
	// and the update failing with ErrDatasetNotFound.
	// In this scenario we re-try the get + update before releasing the lock.
	// Note that the lock and ETag will also protect against race conditions with instance endpoints,
	// which may also modify the same instance collection in the database.
	if err := doUpdate(); err != nil {
		if err == errs.ErrDatasetNotFound {
			log.Info(ctx, "instance document in database corresponding to dataset version was modified before the lock was acquired, retrying...", data)

			if versionUpdate != nil {
				if versionUpdate.Type == models.Static.String() {
					currentVersion, err = api.dataStore.Backend.GetVersionStatic(ctx, versionDetails.datasetID, versionDetails.edition, versionNumber, "")
					if err != nil {
						log.Error(ctx, "putVersion endpoint: datastore.GetVersionStatic returned an error", err, data)
						return nil, nil, nil, err
					}
				} else {
					currentVersion, err = api.dataStore.Backend.GetVersion(ctx, versionDetails.datasetID, versionDetails.edition, versionNumber, "")
					if err != nil {
						log.Error(ctx, "putVersion endpoint: datastore.GetVersion returned an error", err, data)
						return nil, nil, nil, err
					}
				}
			}

			if err = doUpdate(); err != nil {
				log.Error(ctx, "putVersion endpoint: failed to update version document on 2nd attempt", err, data)
				return nil, nil, nil, err
			}
		} else {
			return nil, nil, nil, err
		}
	}

	data["type"] = currentVersion.Type
	data["reqID"] = reqID
	log.Info(ctx, "update version completed successfully", data)
	return currentDataset, currentVersion, combinedVersionUpdate, nil
}

// TODO: Refactor this to reduce the complexity
//
//nolint:gocyclo,gocognit // high cyclomactic & cognitive complexity not in scope for maintenance
func (api *DatasetAPI) publishVersion(
	ctx context.Context,
	currentDataset *models.DatasetUpdate, // Called Dataset in Mongo
	currentVersion *models.Version, // Called Instances in Mongo
	versionUpdate *models.Version, // Next version, that is the new version
	versionDetails VersionDetails, // Struct holding URL Params.
) error {
	data := versionDetails.baseLogData()
	log.Info(ctx, "attempting to publish version", data)
	err := func() error {
		editionDoc, err := api.dataStore.Backend.GetEdition(ctx, versionDetails.datasetID, versionDetails.edition, "")
		if err != nil {
			log.Error(ctx, "putVersion endpoint: failed to find the edition we're trying to update", err, data)
			return err
		}

		editionDoc.Next.State = models.PublishedState
		if err := editionDoc.PublishLinks(ctx, versionUpdate.Links.Version); err != nil {
			log.Error(ctx, "putVersion endpoint: failed to update the edition links for the version we're trying to publish", err, data)
			return err
		}

		editionDoc.Current = editionDoc.Next

		if err := api.dataStore.Backend.UpsertEdition(ctx, versionDetails.datasetID, versionDetails.edition, editionDoc); err != nil {
			log.Error(ctx, "putVersion endpoint: failed to update edition during publishing", err, data)
			return err
		}

		if err := api.dataStore.Backend.SetInstanceIsPublished(ctx, versionUpdate.ID); err != nil {
			if user := dprequest.User(ctx); user != "" {
				data[reqUser] = user
			}
			if caller := dprequest.Caller(ctx); caller != "" {
				data[reqCaller] = caller
			}
			err := errors.WithMessage(err, "putVersion endpoint: failed to set instance node is_published")
			log.Error(ctx, "failed to publish instance version", err, data)
			return err
		}

		// Pass in newVersion variable to include relevant data needed for update on dataset API (e.g. links)
		if err := api.publishDataset(ctx, currentDataset, versionUpdate); err != nil {
			log.Error(ctx, "putVersion endpoint: failed to update dataset document once version state changes to publish", err, data)
			return err
		}
		data["type"] = currentVersion.Type
		data["version_update"] = versionUpdate
		log.Info(ctx, "putVersion endpoint: published version", data)

		// Only want to generate downloads again if there is no public link available
		if currentVersion.Downloads != nil && currentVersion.Downloads.CSV != nil && currentVersion.Downloads.CSV.Public == "" {
			// Lookup the download generator using the version document type
			t, err := models.GetDatasetType(currentVersion.Type)
			if err != nil {
				return fmt.Errorf("error getting type of version: %w", err)
			}
			generator, ok := api.downloadGenerators[t]
			if !ok {
				return fmt.Errorf("no downloader available for type %s", t)
			}
			// Send Kafka message.  The generator which is used depends on the type defined in VersionDoc.
			if err := generator.Generate(ctx, versionDetails.datasetID, versionUpdate.ID, versionDetails.edition, versionDetails.version); err != nil {
				data["instance_id"] = versionUpdate.ID
				data["state"] = versionUpdate.State
				data["type"] = t.String()
				log.Error(ctx, "putVersion endpoint: error while attempting to generate full dataset version downloads on version publish", err, data)
				return err
				// TODO - TECH DEBT - need to add an error event for this.  Kafka message perhaps.
			}
			log.Info(ctx, "putVersion endpoint (publishVersions): generated full dataset version downloads:", data)
		}
		return nil
	}()

	if err != nil {
		return err
	}

	log.Info(ctx, "publish version completed successfully", data)
	return nil
}

// TODO: Refactor this to reduce the complexity
//
//nolint:gocyclo,gocognit // high cyclomactic & cognitive complexity not in scope for maintenance
func populateNewVersionDoc(currentVersion, originalVersion *models.Version) (*models.Version, error) {
	var version models.Version
	err := copier.Copy(&version, originalVersion) // create local copy that escapes to the HEAP at the end of this function
	if err != nil {
		return nil, err
	}

	var alerts []models.Alert

	if version.Alerts != nil {
		alerts = append(alerts, *version.Alerts...)
		for i := range alerts {
			alerts[i].Date = currentVersion.ReleaseDate
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
		latestChanges = append(latestChanges, *currentVersion.LatestChanges...)
	}

	if version.LatestChanges != nil {
		latestChanges = append(latestChanges, *version.LatestChanges...)
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
	version.Links = nil
	if currentVersion.Links != nil {
		version.Links = currentVersion.Links.DeepCopy()
	}

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

	log.Info(context.Background(), "DEBUG", log.Data{"downloads": version.Downloads, "currentDownloads": currentVersion.Downloads})
	if version.Downloads == nil {
		version.Downloads = currentVersion.Downloads
	} else {
		if version.Downloads.XLS == nil && currentVersion.Downloads != nil {
			version.Downloads.XLS = currentVersion.Downloads.XLS
		}

		if version.Downloads.XLSX == nil && currentVersion.Downloads != nil {
			version.Downloads.XLSX = currentVersion.Downloads.XLSX
		}

		if version.Downloads.CSV == nil && currentVersion.Downloads != nil {
			version.Downloads.CSV = currentVersion.Downloads.CSV
		}

		if version.Downloads.CSVW == nil && currentVersion.Downloads != nil {
			version.Downloads.CSVW = currentVersion.Downloads.CSVW
		}

		if version.Downloads.TXT == nil && currentVersion.Downloads != nil {
			version.Downloads.TXT = currentVersion.Downloads.TXT
		}
	}

	if version.UsageNotes == nil {
		version.UsageNotes = currentVersion.UsageNotes
	}

	return &version, nil
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
	case strings.HasPrefix(err.Error(), "state not allowed to transition"):
		status = http.StatusBadRequest
	default:
		err = fmt.Errorf("%s: %w", errs.ErrInternalServer.Error(), err)
		status = http.StatusInternalServerError
	}

	if data == nil {
		data = log.Data{}
	}

	log.Error(ctx, "request unsuccessful", err, data)
	http.Error(w, err.Error(), status)
}

// condensed api call to add new version
//
//nolint:gocyclo,gocognit // high cyclomactic & cognitive complexity not in scope for maintenance
func (api *DatasetAPI) addDatasetVersionCondensed(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition}

	log.Info(ctx, "condensed endpoint called", logData)

	// Validate dataset existence
	if err := api.dataStore.Backend.CheckDatasetExists(ctx, datasetID, ""); err != nil {
		log.Error(ctx, "failed to find dataset", err, logData)
		handleDatasetAPIErr(ctx, errs.ErrDatasetNotFound, w, nil)
		return
	}

	latestVersion, err := api.dataStore.Backend.GetLatestVersionStatic(ctx, datasetID, edition, "")

	if err != nil && !errors.Is(err, errs.ErrVersionNotFound) {
		log.Error(ctx, "failed to check latest version", err, logData)
		handleDatasetAPIErr(ctx, errs.ErrInternalServer, w, nil)
		return
	}

	if err == nil && latestVersion.State != models.PublishedState {
		log.Error(ctx, "unpublished version already exists", errors.New("cannot create new version when unpublished version exists"),
			log.Data{"state": latestVersion.State, "version": latestVersion.Version})

		errorResponse := map[string]interface{}{
			"error":               "cannot create new version when an unpublished version already exists",
			"unpublished_version": latestVersion.Version,
		}

		responseBody, err := json.Marshal(errorResponse)
		if err != nil {
			log.Error(ctx, "failed to marshal error response", err, logData)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		setJSONContentType(w)
		w.WriteHeader(http.StatusBadRequest)
		if _, err := w.Write(responseBody); err != nil {
			log.Error(ctx, "failed to write response", err, logData)
		}
		return
	}

	versionRequest := &models.Version{}
	if err := json.NewDecoder(r.Body).Decode(versionRequest); err != nil {
		log.Error(ctx, "failed to unmarshal version", err, logData)
		handleVersionAPIErr(ctx, errs.ErrUnableToParseJSON, w, logData)
		return
	}

	if missingFields := validateVersionFields(versionRequest); len(missingFields) > 0 {
		log.Error(ctx, "failed validation check for version update", fmt.Errorf("missing mandatory version fields"), logData)

		errorResponse := map[string]interface{}{
			"error":          "missing mandatory version fields",
			"missing_fields": missingFields,
		}

		responseBody, err := json.Marshal(errorResponse)
		if err != nil {
			log.Error(ctx, "failed to marshal error response", err, logData)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		setJSONContentType(w)
		w.WriteHeader(http.StatusBadRequest)
		if _, err := w.Write(responseBody); err != nil {
			log.Error(ctx, "failed to write response", err, logData)
		}
		return
	}

	var nextVersion int
	versionRequest.Edition = edition

	if errors.Is(err, errs.ErrVersionNotFound) {
		log.Warn(ctx, "edition not found, defaulting to version 1", logData)
		nextVersion = 1
	} else {
		nextVersion, err = api.dataStore.Backend.GetNextVersionStatic(ctx, datasetID, edition)
		if err != nil {
			log.Error(ctx, "failed to get next version", err, logData)
			handleDatasetAPIErr(ctx, errs.ErrInternalServer, w, nil)
			return
		}
	}

	versionRequest.State = models.AssociatedState
	versionRequest.Version = nextVersion
	versionRequest.DatasetID = datasetID
	versionRequest.Links = api.generateVersionLinks(datasetID, edition, nextVersion, versionRequest.Links)

	// Store version in 'versions' collection
	newVersion, err := api.dataStore.Backend.AddVersionStatic(ctx, versionRequest)
	if err != nil {
		log.Error(ctx, "failed to add version", err, logData)
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	datasetDoc, err := api.dataStore.Backend.GetDataset(ctx, datasetID)
	if err != nil {
		log.Error(ctx, "failed to get dataset", err, logData)
		handleDatasetAPIErr(ctx, errs.ErrInternalServer, w, nil)
		return
	}

	datasetDoc.Next.LastUpdated = newVersion.LastUpdated
	datasetDoc.Next.State = models.AssociatedState

	if err := api.dataStore.Backend.UpsertDataset(ctx, datasetID, datasetDoc); err != nil {
		log.Error(ctx, "failed to update dataset", err, logData)
		handleDatasetAPIErr(ctx, errs.ErrInternalServer, w, nil)
		return
	}

	log.Info(ctx, "add version: request successful", logData)

	setJSONContentType(w)
	dpresponse.SetETag(w, newVersion.ETag)
	w.WriteHeader(http.StatusCreated)

	response, err := json.Marshal(newVersion)
	if err != nil {
		log.Error(ctx, "failed to marshal version to JSON", err, logData)
		handleDatasetAPIErr(ctx, errs.ErrInternalServer, w, nil)
		return
	}

	if _, err := w.Write(response); err != nil {
		log.Error(ctx, "failed to write response", err, logData)
	}
}

func validateVersionFields(version *models.Version) []string {
	var missingFields []string

	if version.ReleaseDate == "" {
		missingFields = append(missingFields, "release_date")
	}
	if version.Distributions == nil || len(*version.Distributions) == 0 {
		missingFields = append(missingFields, "distributions")
	}
	if version.EditionTitle == "" {
		missingFields = append(missingFields, "edition_title")
	}
	return missingFields
}

func (api *DatasetAPI) generateVersionLinks(datasetID, edition string, version int, existingLinks *models.VersionLinks) *models.VersionLinks {
	spatial := (*models.LinkObject)(nil)

	if existingLinks != nil && existingLinks.Spatial != nil {
		spatial = existingLinks.Spatial
	}
	return &models.VersionLinks{
		Dataset: &models.LinkObject{
			HRef: fmt.Sprintf("%s/datasets/%s", api.host, datasetID),
			ID:   datasetID,
		},
		Self: &models.LinkObject{
			HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%d", api.host, datasetID, edition, version),
		},
		Edition: &models.LinkObject{
			HRef: fmt.Sprintf("%s/datasets/%s/editions/%s", api.host, datasetID, edition),
			ID:   edition,
		},
		Version: &models.LinkObject{
			HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%d", api.host, datasetID, edition, version),
			ID:   fmt.Sprintf("%d", version),
		},
		Spatial: spatial,
	}
}

func (api *DatasetAPI) putState(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)

	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]

	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	log.Info(ctx, "putState endpoint: endpoint called", logData)

	versionID, err := models.ParseAndValidateVersionNumber(ctx, version)
	if err != nil {
		log.Error(ctx, "putState endpoint: invalid version request", err, logData)
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	var stateUpdate models.StateUpdate
	if err := json.NewDecoder(r.Body).Decode(&stateUpdate); err != nil {
		log.Error(ctx, "putState endpoint: failed to unmarshal state update", err, logData)
		handleVersionAPIErr(ctx, errs.ErrUnableToParseJSON, w, logData)
		return
	}

	if err = models.CheckState("version", stateUpdate.State); err != nil {
		log.Error(ctx, "putState endpoint: state is invalid", err, log.Data{"state": stateUpdate.State})
		handleVersionAPIErr(ctx, models.ErrVersionStateInvalid, w, logData)
		return
	}

	currentVersion, err := api.dataStore.Backend.GetVersionStatic(ctx, datasetID, edition, versionID, "")
	if err != nil {
		log.Error(ctx, "putState endpoint: failed to get version", err, logData)
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	if currentVersion != nil {
		currentVersion.State = stateUpdate.State
		currentVersion.Type = models.Static.String()
	}

	err = api.smDatasetAPI.AmendVersion(r.Context(), vars, currentVersion)
	if err != nil {
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	if stateUpdate.State == models.PublishedState && currentVersion.Distributions != nil && len(*currentVersion.Distributions) > 0 {
		err = api.publishDistributionFiles(ctx, currentVersion, logData)
		if err != nil {
			log.Error(ctx, "putState endpoint: failed to publish distribution files", err, logData)
			handleVersionAPIErr(ctx, err, w, logData)
			return
		}
	}

	setJSONContentType(w)
	w.WriteHeader(http.StatusOK)
	log.Info(ctx, "putState endpoint: request successful", logData)
}

func (api *DatasetAPI) publishDistributionFiles(ctx context.Context, version *models.Version, logData log.Data) error {
	if api.filesAPIClient == nil {
		return fmt.Errorf("files API client not configured")
	}

	if version.Distributions == nil || len(*version.Distributions) == 0 {
		return nil
	}

	var lastError error
	totalFiles := len(*version.Distributions)
	successCount := 0

	for _, distribution := range *version.Distributions {
		if distribution.DownloadURL == "" {
			continue
		}

		filepath := distribution.DownloadURL

		fileLogData := log.Data{
			"filepath":            filepath,
			"distribution_title":  distribution.Title,
			"distribution_format": distribution.Format,
		}

		maps.Copy(fileLogData, logData)

		fileMetadata, err := api.filesAPIClient.GetFile(ctx, filepath, api.authToken)
		if err != nil {
			log.Error(ctx, "failed to get file metadata", err, fileLogData)
			lastError = err
			continue
		}

		err = api.filesAPIClient.MarkFilePublished(ctx, filepath, fileMetadata.Etag)
		if err != nil {
			log.Error(ctx, "failed to publish file", err, fileLogData)
			lastError = err
			continue
		}

		successCount++
		log.Info(ctx, "successfully published file", fileLogData)
	}

	log.Info(ctx, "completed publishing distribution files", log.Data{
		"total_files": totalFiles,
		"successful":  successCount,
		"failed":      totalFiles - successCount,
	})

	if lastError != nil {
		return fmt.Errorf("one or more errors occurred while publishing files: %w", lastError)
	}

	return nil
}
