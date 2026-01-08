package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"

	"github.com/ONSdigital/dp-api-clients-go/v2/headers"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/utils"
	kafka "github.com/ONSdigital/dp-kafka/v4"
	dpresponse "github.com/ONSdigital/dp-net/v3/handlers/response"
	dphttp "github.com/ONSdigital/dp-net/v3/http"
	"github.com/ONSdigital/dp-net/v3/links"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
)

var (
	// errors that map to a HTTP 404 response
	notFound = map[error]bool{
		errs.ErrDatasetNotFound:  true,
		errs.ErrEditionNotFound:  true,
		errs.ErrEditionsNotFound: true,
		errs.ErrVersionNotFound:  true,
		errs.ErrVersionsNotFound: true,
	}

	// errors that map to a HTTP 400 response
	badRequest = map[error]bool{
		errs.ErrUnableToParseJSON:                      true,
		models.ErrPublishedVersionCollectionIDInvalid:  true,
		models.ErrAssociatedVersionCollectionIDInvalid: true,
		models.ErrVersionStateInvalid:                  true,
		errs.ErrInvalidBody:                            true,
		errs.ErrInvalidQueryParameter:                  true,
		errs.ErrSpacesNotAllowedInID:                   true,
	}

	// HTTP 500 responses with a specific message
	internalServerErrWithMessage = map[error]bool{
		errs.ErrResourceState: true,
	}
)

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
		authorised := api.checkUserPermission(r, logData, datasetEditionVersionReadPermission)
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
func (api *DatasetAPI) getVersion(w http.ResponseWriter, r *http.Request) (*models.SuccessResponse, *models.ErrorResponse) {
	ctx := r.Context()

	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	versionNumber := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": versionNumber}

	v, getVersionErr := func() (*models.Version, error) {
		authorised := api.checkUserPermission(r, logData, datasetEditionVersionReadPermission)

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
		responseError := models.NewError(getVersionErr, getVersionErr.Error(), "internal error")
		return nil, models.NewErrorResponse(getVersionAPIErrStatusCode(getVersionErr), nil, responseError)
	}

	if api.enableURLRewriting {
		datasetLinksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetDatasetAPIURL())
		codeListLinksBuilder := links.FromHeadersOrDefault(&r.Header, api.urlBuilder.GetCodeListAPIURL())

		var err error

		err = utils.RewriteVersionLinks(ctx, v.Links, datasetLinksBuilder)
		if err != nil {
			log.Error(ctx, "getVersion endpoint: failed to rewrite version links", err, logData)
			return nil, models.NewErrorResponse(getVersionAPIErrStatusCode(err), nil, models.NewError(err, "failed to rewrite version links", "internal error"))
		}

		v.Dimensions, err = utils.RewriteDimensions(ctx, v.Dimensions, datasetLinksBuilder, codeListLinksBuilder)
		if err != nil {
			log.Error(ctx, "getVersion endpoint: failed to rewrite dimensions", err, logData)
			return nil, models.NewErrorResponse(getVersionAPIErrStatusCode(err), nil, models.NewError(err, "failed to rewrite dimensions", "internal error"))
		}

		err = utils.RewriteDownloadLinks(ctx, v.Downloads, api.urlBuilder.GetDownloadServiceURL())
		if err != nil {
			log.Error(ctx, "getVersion endpoint: failed to rewrite download links", err, logData)
			return nil, models.NewErrorResponse(getVersionAPIErrStatusCode(err), nil, models.NewError(err, "failed to rewrite download links", "internal error"))
		}

		v.Distributions, err = utils.RewriteDistributions(ctx, v.Distributions, api.urlBuilder.GetDownloadServiceURL())
		if err != nil {
			log.Error(ctx, "getVersion endpoint: failed to rewrite distributions DownloadURLs", err, logData)
			return nil, models.NewErrorResponse(getVersionAPIErrStatusCode(err), nil, models.NewError(err, "failed to rewrite distributions DownloadURLs", "internal error"))
		}
	}

	setJSONContentType(w)
	if v.ETag != "" {
		dpresponse.SetETag(w, v.ETag)
	}

	versionBytes, err := json.Marshal(v)
	if err != nil {
		log.Error(ctx, "failed to marshal version resource into bytes", err, logData)
		return nil, models.NewErrorResponse(getVersionAPIErrStatusCode(err), nil, models.NewError(err, "failed to marshal version into bytes", "internal error"))
	}

	responseHeaders := map[string]string{
		"Code": strconv.Itoa(http.StatusOK),
	}

	return models.NewSuccessResponse(versionBytes, http.StatusOK, responseHeaders), nil
}

//nolint:gocognit,gocyclo // high complexity accepted for this handler
func (api *DatasetAPI) putVersion(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)
	data := log.Data{
		"datasetID": vars["dataset_id"],
		"edition":   vars["edition"],
		"version":   vars["version"],
	}

	// Read body once and validate distributions before unmarshaling
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Error(ctx, "failed to read request body", err, data)
		handleVersionAPIErr(ctx, errs.ErrUnableToReadMessage, w, data)
		return
	}

	if err := utils.ValidateDistributionsFromRequestBody(bodyBytes); err != nil {
		log.Error(ctx, "invalid distributions format", err, data)
		handleVersionAPIErr(ctx, err, w, data)
		return
	}

	version, err := models.CreateVersion(bytes.NewReader(bodyBytes), vars["dataset_id"])
	if err != nil {
		handleVersionAPIErr(ctx, err, w, data)
		return
	}

	// check for edition ID/title conflicts, spaces in IDs for static datasets and Populate distributions (static only)
	if version.Type == models.Static.String() {
		if err := utils.PopulateDistributions(version); err != nil {
			handleVersionAPIErr(ctx, err, w, data)
			return
		}

		if version.Edition != "" {
			if err := utils.ValidateIDNoSpaces(version.Edition); err != nil {
				log.Error(ctx, "putVersion endpoint: edition ID in request body contains spaces", err, data)
				handleVersionAPIErr(ctx, err, w, data)
				return
			}
		}

		if version.DatasetID != "" {
			if err := utils.ValidateIDNoSpaces(version.DatasetID); err != nil {
				log.Error(ctx, "putVersion endpoint: edition ID in request body contains spaces", err, data)
				handleVersionAPIErr(ctx, err, w, data)
				return
			}
		}
		if version.Edition != "" || version.EditionTitle != "" {
			versionStr := vars["version"]

			versionNumber, _ := strconv.Atoi(versionStr)
			// Load the existing version
			existingVersion, getErr := api.dataStore.Backend.GetVersionStatic(
				ctx,
				vars["dataset_id"],
				vars["edition"],
				versionNumber,
				"",
			)

			if getErr != nil {
				handleVersionAPIErr(ctx, getErr, w, data)
				return
			}

			// Detect whether edition ID or title has changed
			editionChanged := existingVersion.Edition != version.Edition
			titleChanged := existingVersion.EditionTitle != version.EditionTitle

			// Only validate uniqueness IF edition or title is changing
			if editionChanged {
				checkErr := api.dataStore.Backend.CheckEditionExistsStatic(ctx, version.DatasetID, version.Edition, "")
				if checkErr == nil {
					log.Error(ctx, "edition ID already exists for this dataset", errs.ErrEditionAlreadyExists, data)
					handleVersionAPIErr(ctx, errs.ErrEditionAlreadyExists, w, data)
					return
				} else if !errors.Is(checkErr, errs.ErrEditionNotFound) {
					log.Error(ctx, "failed to check edition ID existence", checkErr, data)
					handleVersionAPIErr(ctx, checkErr, w, data)
					return
				}
			}

			if titleChanged {
				checkErr := api.dataStore.Backend.CheckEditionTitleExistsStatic(ctx, version.DatasetID, version.EditionTitle)
				if checkErr != nil {
					handleVersionAPIErr(ctx, checkErr, w, data)
					return
				}
			}
		}
	}

	var amendedVersion *models.Version

	amendedVersion, err = api.smDatasetAPI.AmendVersion(r.Context(), vars, version)
	if err != nil {
		handleVersionAPIErr(ctx, err, w, data)
		return
	}

	versionBytes, err := json.Marshal(amendedVersion)
	if err != nil {
		log.Error(ctx, "failed to marshal version resource into bytes", err, data)
		handleVersionAPIErr(ctx, err, w, data)
	}

	setJSONContentType(w)
	_, err = w.Write(versionBytes)
	if err != nil {
		log.Error(ctx, "failed writing bytes to response", err, data)
		handleVersionAPIErr(ctx, err, w, data)
	}

	w.WriteHeader(http.StatusOK)
	log.Info(ctx, "putVersion endpoint: request successful", data)
}

func (api *DatasetAPI) deleteVersion(w http.ResponseWriter, r *http.Request) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()
	vars := mux.Vars(r)

	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	versionStr := vars["version"]

	logData := log.Data{
		"dataset_id": datasetID,
		"edition":    edition,
		"version":    versionStr,
	}

	versionNum, err := models.ParseAndValidateVersionNumber(ctx, versionStr)
	if err != nil {
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	isStatic, err := api.dataStore.Backend.IsStaticDataset(ctx, datasetID)
	if err != nil {
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	if isStatic {
		if !api.enableDeleteStaticVersion {
			handleVersionAPIErr(ctx, errs.ErrMethodNotAllowed, w, logData)
			return
		}

		if !api.checkUserPermission(r, logData, datasetEditionVersionDeletePermission) {
			handleVersionAPIErr(ctx, errs.ErrUnauthorised, w, logData)
			return
		}

		if err := api.smDatasetAPI.DeleteStaticVersion(ctx, datasetID, edition, versionNum, api.filesAPIClient); err != nil {
			handleVersionAPIErr(ctx, err, w, logData)
			return
		}

		w.WriteHeader(http.StatusNoContent)
		log.Info(ctx, "deleteVersion: successfully deleted static version", logData)
		return
	}

	if !api.enableDetachDataset {
		handleVersionAPIErr(ctx, errs.ErrMethodNotAllowed, w, logData)
		return
	}

	api.detachVersion(w, r)
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
		authorised := api.checkUserPermission(r, logData, datasetEditionVersionDeletePermission)
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
	if data == nil {
		data = log.Data{}
	}

	status := getVersionAPIErrStatusCode(err)
	if status == http.StatusInternalServerError && !internalServerErrWithMessage[err] {
		err = fmt.Errorf("%s: %w", errs.ErrInternalServer.Error(), err)
	}

	log.Error(ctx, "request unsuccessful", err, data)
	http.Error(w, err.Error(), status)
}

func getVersionAPIErrStatusCode(err error) int {
	var status int
	switch {
	case notFound[err] || errs.NotFoundMap[err]:
		status = http.StatusNotFound
	case badRequest[err] || errs.BadRequestMap[err]:
		status = http.StatusBadRequest
	case errs.ConflictRequestMap[err]:
		status = http.StatusConflict
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
	case strings.HasPrefix(err.Error(), "a published version cannot be deleted"):
		status = http.StatusForbidden
	case strings.Contains(err.Error(), "format field is missing"):
		status = http.StatusBadRequest
	case strings.Contains(err.Error(), "format field is invalid"):
		status = http.StatusBadRequest
	case errs.NotAllowedMap[err]:
		status = http.StatusMethodNotAllowed
	default:
		status = http.StatusInternalServerError
	}

	return status
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

	// Create a version update with the target state
	versionUpdate := &models.Version{
		ID:    currentVersion.ID,
		State: stateUpdate.State,
		Type:  models.Static.String(),
	}

	updatedVersion, err := api.smDatasetAPI.AmendVersion(r.Context(), vars, versionUpdate)
	if err != nil {
		handleVersionAPIErr(ctx, err, w, logData)
		return
	}

	if stateUpdate.State == models.PublishedState && updatedVersion.Distributions != nil && len(*updatedVersion.Distributions) > 0 {
		err = api.publishDistributionFiles(ctx, updatedVersion, logData)
		if err != nil {
			log.Error(ctx, "putState endpoint: failed to publish distribution files", err, logData)
			handleVersionAPIErr(ctx, err, w, logData)
			return
		}
	}

	if updatedVersion.State == models.PublishedState {
		searchContentUpdatedEvent := map[string]interface{}{
			"dataset_id":   datasetID,
			"uri":          fmt.Sprintf("/datasets/%s/editions/%s/versions/%s", datasetID, edition, version),
			"title":        currentVersion.EditionTitle,
			"edition":      currentVersion.Edition,
			"content_type": "dataset",
			"release_date": currentVersion.ReleaseDate,
		}

		jsonBytes, err := json.Marshal(searchContentUpdatedEvent)
		logData["search_content_updated_event"] = searchContentUpdatedEvent
		if err != nil {
			log.Error(ctx, "failed to marshal searchContentUpdatedEvent for kafka", err, logData)
			handleVersionAPIErr(ctx, err, w, logData)
			return
		} else {
			go func() {
				api.searchContentUpdatedProducer.Producer.Output() <- kafka.BytesMessage{Value: jsonBytes, Context: ctx}
			}()
			log.Info(ctx, "putState endpoint: queued search content update for kafka", logData)
		}
	}

	// Purge Cloudflare cache if enabled and version is being published
	if api.cloudflareEnabled && stateUpdate.State == models.PublishedState {
		prefixes := utils.GeneratePurgePrefixes(api.urlBuilder.GetWebsiteURL().String(), api.urlBuilder.GetAPIRouterPublicURL().String(), datasetID, edition, version)
		logData["purge_prefixes"] = prefixes

		err := api.cloudflareClient.PurgeByPrefixes(ctx, prefixes)
		if err != nil {
			log.Error(ctx, "putState endpoint: failed to purge cache by prefixes", err, logData)
		} else {
			log.Info(ctx, "putState endpoint: successfully purged cache by prefixes", logData)
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
	var filesAPIError error
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

		_, err := api.filesAPIClient.GetFile(ctx, filepath)
		if err != nil {
			log.Error(ctx, "failed to get file metadata", err, fileLogData)

			if strings.Contains(err.Error(), "FileNotRegistered") ||
				strings.Contains(err.Error(), "file not registered") ||
				strings.Contains(err.Error(), "not found") {
				filesAPIError = errs.ErrFileMetadataNotFound
			}
			lastError = err
			continue
		}

		err = api.filesAPIClient.MarkFilePublished(ctx, filepath)
		if err != nil {
			log.Error(ctx, "failed to publish file", err, fileLogData)

			if strings.Contains(err.Error(), "FileStateError") ||
				strings.Contains(err.Error(), "file is not set as publishable") ||
				strings.Contains(err.Error(), "file state is not in state uploaded") {
				filesAPIError = errs.ErrFileNotInCorrectState
			}
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

	if filesAPIError != nil {
		return filesAPIError
	}

	if lastError != nil {
		return fmt.Errorf("one or more errors occurred while publishing files: %w", lastError)
	}

	return nil
}
