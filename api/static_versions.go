package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/utils"
	dpresponse "github.com/ONSdigital/dp-net/v3/handlers/response"
	dphttp "github.com/ONSdigital/dp-net/v3/http"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

//nolint:gocritic // return type must be interface{} for paginator but our results are of type []*models.DatasetEdition
func (api *DatasetAPI) getDatasetEditions(w http.ResponseWriter, r *http.Request, limit, offset int) (interface{}, int, error) {
	ctx := r.Context()
	logData := log.Data{}

	stateParam := r.URL.Query().Get("state")
	publishedOnly := r.URL.Query().Get("published")

	if stateParam != "" && publishedOnly != "" {
		log.Error(ctx, "getDatasetEditions endpoint: cannot request state and published parameters at the same time", errs.ErrInvalidParamCombination, logData)
		handleVersionAPIErr(ctx, errs.ErrInvalidParamCombination, w, logData)
		return nil, 0, errs.ErrInvalidParamCombination
	}

	if stateParam != "" {
		logData["state"] = stateParam
		if err := models.CheckState("", stateParam); err != nil {
			log.Error(ctx, "getDatasetEditions endpoint: invalid state parameter", err, logData)
			handleVersionAPIErr(ctx, errs.ErrInvalidQueryParameter, w, logData)
			return nil, 0, errs.ErrInvalidQueryParameter
		}
	}

	if publishedOnly != "" {
		logData["publishedOnly"] = publishedOnly
		_, err := strconv.ParseBool(publishedOnly)
		if err != nil {
			log.Error(ctx, "getDatasetEditions endpoint: invalid published parameter", err, logData)
			handleVersionAPIErr(ctx, errs.ErrInvalidQueryParameter, w, logData)
			return nil, 0, errs.ErrInvalidQueryParameter
		}
	}

	// need to use the string value of published here if it's provided as the boolean ParseBool function does not allow nil values and this could be nil
	versions, totalCount, err := api.dataStore.Backend.GetStaticVersionsByState(ctx, stateParam, publishedOnly, offset, limit)
	if err != nil {
		if errors.Is(err, errs.ErrVersionsNotFound) {
			log.Error(ctx, "getDatasetEditions endpoint: no versions found", err, logData)
			handleVersionAPIErr(ctx, errs.ErrVersionsNotFound, w, logData)
			return nil, 0, errs.ErrVersionsNotFound
		}
		log.Error(ctx, "getDatasetEditions endpoint: failed to get versions", err, logData)
		handleVersionAPIErr(ctx, errs.ErrInternalServer, w, logData)
		return nil, 0, errs.ErrInternalServer
	}

	results := make([]*models.DatasetEdition, 0, len(versions))

	for _, version := range versions {
		logData["dataset_id"] = version.Links.Dataset.ID
		logData["edition"] = version.Edition
		logData["version"] = version.Version

		dataset, err := api.dataStore.Backend.GetDataset(ctx, version.Links.Dataset.ID)
		if err != nil {
			if errors.Is(err, errs.ErrDatasetNotFound) {
				log.Error(ctx, "getDatasetEditions endpoint: dataset not found", err, logData)
				handleVersionAPIErr(ctx, errs.ErrDatasetNotFound, w, logData)
				return nil, 0, errs.ErrDatasetNotFound
			}
			log.Error(ctx, "getDatasetEditions endpoint: failed to get dataset", err, logData)
			handleVersionAPIErr(ctx, errs.ErrInternalServer, w, logData)
			return nil, 0, errs.ErrInternalServer
		}

		results = append(results, &models.DatasetEdition{
			DatasetID:    dataset.ID,
			Title:        dataset.Next.Title,
			Description:  dataset.Next.Description,
			Edition:      version.Edition,
			EditionTitle: version.EditionTitle,
			LatestVersion: models.LinkObject{
				HRef: fmt.Sprintf("/datasets/%s/editions/%s/versions/%d", dataset.ID, version.Edition, version.Version),
				ID:   strconv.Itoa(version.Version),
			},
			ReleaseDate: version.ReleaseDate,
			State:       version.State,
		})
	}

	return results, totalCount, nil
}

// condensed api call to add new version
//
//nolint:gocyclo,gocognit // high cyclomactic & cognitive complexity not in scope for maintenance
func (api *DatasetAPI) addDatasetVersionCondensed(w http.ResponseWriter, r *http.Request) (*models.SuccessResponse, *models.ErrorResponse) {
	defer dphttp.DrainBody(r)

	ctx := r.Context()

	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition}

	log.Info(ctx, "condensed endpoint called", logData)

	if err := utils.ValidateIDNoSpaces(datasetID); err != nil {
		log.Error(ctx, "addDatasetVersionCondensed endpoint: dataset ID contains spaces", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewError(err, errs.ErrSpacesNotAllowedInID.Error(), errs.ErrSpacesNotAllowedInID.Error()))
	}

	if err := utils.ValidateIDNoSpaces(edition); err != nil {
		log.Error(ctx, "addDatasetVersionCondensed endpoint: edition ID contains spaces", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewError(err, errs.ErrSpacesNotAllowedInID.Error(), errs.ErrSpacesNotAllowedInID.Error()))
	}

	// Read body once and validate distributions before unmarshaling
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Error(ctx, "failed to read request body", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewError(err, "failed to read request body", "failed to read request body"))
	}

	if err := utils.ValidateDistributionsFromRequestBody(bodyBytes); err != nil {
		log.Error(ctx, "invalid distributions format", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewValidationError(models.ErrMissingParameters, err.Error()))
	}

	versionRequest := &models.Version{}
	if err := json.Unmarshal(bodyBytes, versionRequest); err != nil {
		log.Error(ctx, "failed to unmarshal version", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewError(err, models.JSONUnmarshalError, "failed to unmarshal version"))
	}

	if missingFields := validateVersionFields(versionRequest); len(missingFields) > 0 {
		log.Error(ctx, "failed validation check for version update", fmt.Errorf("missing mandatory version fields"), logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewValidationError(models.ErrMissingParameters, models.ErrMissingParametersDescription+" "+strings.Join(missingFields, " ")))
	}

	if err := utils.PopulateDistributions(versionRequest); err != nil {
		log.Error(ctx, "failed to populate distributions", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewValidationError(models.ErrMissingParameters, err.Error()))
	}

	// validate versiontype
	if versionRequest.Type != "" && versionRequest.Type != models.Static.String() {
		log.Error(ctx, "addDatasetVersionCondensed endpoint: only allowed to create static type versions", errs.ErrInvalidBody, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewValidationError(models.ErrInvalidTypeError, models.ErrTypeNotStaticDescription))
	}

	// Validate dataset existence
	if err := api.dataStore.Backend.CheckDatasetExists(ctx, datasetID, ""); err != nil {
		log.Error(ctx, "failed to find dataset", err, logData)
		return nil, models.NewErrorResponse(http.StatusNotFound, nil, models.NewValidationError(models.ErrDatasetNotFound, models.ErrDatasetNotFoundDescription))
	}

	latestVersion, err := api.dataStore.Backend.GetLatestVersionStatic(ctx, datasetID, edition, "")

	if err != nil && !errors.Is(err, errs.ErrVersionNotFound) {
		log.Error(ctx, "failed to check latest version", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(err, "failed to check latest version", "internal error"))
	}

	var nextVersion int
	versionRequest.Edition = edition

	if errors.Is(err, errs.ErrVersionNotFound) {
		log.Warn(ctx, "edition not found, defaulting to version 1", logData)
		// Creating version 1 of a new edition
		// Check edition ID
		checkErr := api.dataStore.Backend.CheckEditionExistsStatic(ctx, datasetID, edition, "")
		if checkErr == nil {
			log.Error(ctx, "edition ID already exists", errs.ErrEditionAlreadyExists, logData)
			return nil, models.NewErrorResponse(http.StatusConflict, nil, models.NewValidationError(models.ErrEditionAlreadyExists, models.ErrEditionAlreadyExistsDescription))
		} else if !errors.Is(checkErr, errs.ErrEditionNotFound) {
			log.Error(ctx, "failed to check edition ID existence", checkErr, logData)
			return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(checkErr, "failed to check edition ID", "internal error"))
		}
		// Check edition title
		checkErr = api.dataStore.Backend.CheckEditionTitleExistsStatic(ctx, datasetID, versionRequest.EditionTitle)
		if checkErr != nil {
			if errors.Is(checkErr, errs.ErrEditionTitleAlreadyExists) {
				log.Error(ctx, "edition title already exists", checkErr, logData)
				return nil, models.NewErrorResponse(http.StatusConflict, nil, models.NewValidationError(models.ErrEditionTitleAlreadyExists, models.ErrEditionTitleAlreadyExistsDescription))
			}
			log.Error(ctx, "failed to check edition title existence", checkErr, logData)
			return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(checkErr, "failed to check edition title", "internal error"))
		}
		nextVersion = 1
	} else {
		nextVersion = latestVersion.Version + 1
	}

	if err == nil && latestVersion.State != models.PublishedState {
		log.Error(ctx, "unpublished version already exists", errors.New("cannot create new version when unpublished version exists"),
			log.Data{"state": latestVersion.State, "version": latestVersion.Version})

		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewError(err, models.ErrVersionAlreadyExists, models.ErrUnpublishedVersionAlreadyExistsDescription+" - unpublished_version: "+strconv.Itoa(latestVersion.Version)))
	}

	versionRequest.State = models.AssociatedState
	versionRequest.Version = nextVersion
	versionRequest.DatasetID = datasetID
	versionRequest.Links = api.generateVersionLinks(datasetID, edition, nextVersion, versionRequest.Links)
	versionRequest.Type = models.Static.String()

	// Store version in 'versions' collection
	newVersion, err := api.dataStore.Backend.AddVersionStatic(ctx, versionRequest)
	if err != nil {
		log.Error(ctx, "failed to add version", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(err, "failed to add version", "internal error"))
	}

	datasetDoc, err := api.dataStore.Backend.GetDataset(ctx, datasetID)
	if err != nil {
		log.Error(ctx, "failed to get dataset", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(err, "failed to get dataset", "internal error"))
	}

	datasetDoc.Next.LastUpdated = newVersion.LastUpdated
	datasetDoc.Next.State = models.AssociatedState

	if err := api.dataStore.Backend.UpsertDataset(ctx, datasetID, datasetDoc); err != nil {
		log.Error(ctx, "failed to update dataset", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(err, "failed to update dataset", "internal error"))
	}

	log.Info(ctx, "add version: request successful", logData)

	dpresponse.SetETag(w, newVersion.ETag)

	response, err := json.Marshal(newVersion)
	if err != nil {
		log.Error(ctx, "failed to marshal version to JSON", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(errs.ErrInternalServer, "failed to marshal version to JSON", "internal error"))
	}

	headers := map[string]string{
		"Code": strconv.Itoa(http.StatusCreated),
	}
	return models.NewSuccessResponse(response, http.StatusCreated, headers), nil
}

func (api *DatasetAPI) createVersion(w http.ResponseWriter, r *http.Request) (*models.SuccessResponse, *models.ErrorResponse) {
	ctx := r.Context()

	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]

	logData := log.Data{
		"dataset_id": datasetID,
		"edition":    edition,
		"version":    version,
	}

	if err := utils.ValidateIDNoSpaces(datasetID); err != nil {
		log.Error(ctx, "createVersion endpoint: dataset ID contains spaces", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewError(err, errs.ErrSpacesNotAllowedInID.Error(), errs.ErrSpacesNotAllowedInID.Error()))
	}

	if err := utils.ValidateIDNoSpaces(edition); err != nil {
		log.Error(ctx, "createVersion endpoint: edition ID contains spaces", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewError(err, errs.ErrSpacesNotAllowedInID.Error(), errs.ErrSpacesNotAllowedInID.Error()))
	}

	// Read body once and validate distributions before unmarshaling
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Error(ctx, "createVersion endpoint: failed to read request body", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewError(err, "failed to read request body", "failed to read request body"))
	}

	if err := utils.ValidateDistributionsFromRequestBody(bodyBytes); err != nil {
		log.Error(ctx, "createVersion endpoint: invalid distributions format", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewValidationError(models.ErrMissingParameters, err.Error()))
	}

	newVersion := &models.Version{}
	if err := json.Unmarshal(bodyBytes, newVersion); err != nil {
		log.Error(ctx, "createVersion endpoint: failed to unmarshal version", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewError(err, models.JSONUnmarshalError, "failed to unmarshal version"))
	}

	if err := utils.PopulateDistributions(newVersion); err != nil {
		log.Error(ctx, "createVersion endpoint: failed to populate distributions", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewValidationError(models.ErrMissingParameters, err.Error()))
	}

	versionNumber, err := strconv.Atoi(version)
	if err != nil || versionNumber < 1 {
		log.Error(ctx, "createVersion endpoint: invalid version parameter", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewError(err, models.ErrInvalidQueryParameter, models.ErrInvalidQueryParameterDescription+": version"))
	}

	if newVersion.Type != models.Static.String() {
		log.Error(ctx, "createVersion endpoint: only allowed to create static type versions", errs.ErrInvalidBody, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewValidationError(models.ErrInvalidTypeError, models.ErrTypeNotStaticDescription))
	}

	// set mandatory fields
	newVersion.DatasetID = datasetID
	newVersion.Edition = edition
	newVersion.Version = versionNumber
	newVersion.Type = models.Static.String()
	newVersion.State = models.AssociatedState
	newVersion.Links = api.generateVersionLinks(datasetID, edition, versionNumber, nil)

	missingFields := validateVersionFields(newVersion)
	if len(missingFields) > 0 {
		logData["missing_fields"] = missingFields

		validationErrors := []models.Error{}
		for _, field := range missingFields {
			validationErrors = append(validationErrors, models.NewValidationError(models.ErrMissingParameters, models.ErrMissingParametersDescription+": "+field))
		}

		log.Error(ctx, "createVersion endpoint: failed validation check for new version", fmt.Errorf("missing mandatory version fields"), logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, validationErrors...)
	}

	err = api.dataStore.Backend.CheckDatasetExists(ctx, datasetID, "")
	if err != nil {
		if err == errs.ErrDatasetNotFound {
			log.Error(ctx, "createVersion endpoint: dataset not found", err, logData)
			return nil, models.NewErrorResponse(http.StatusNotFound, nil, models.NewValidationError(models.ErrDatasetNotFound, models.ErrDatasetNotFoundDescription))
		}
		log.Error(ctx, "createVersion endpoint: failed to check dataset existence", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(err, models.InternalError, models.InternalErrorDescription))
	}

	err = api.dataStore.Backend.CheckEditionExistsStatic(ctx, datasetID, edition, "")
	if err != nil {
		if err == errs.ErrEditionNotFound {
			log.Error(ctx, "createVersion endpoint: edition not found", err, logData)
			return nil, models.NewErrorResponse(http.StatusNotFound, nil, models.NewValidationError(models.ErrEditionNotFound, models.ErrEditionNotFoundDescription))
		}
		log.Error(ctx, "createVersion endpoint: failed to check edition existence", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(err, models.InternalError, models.InternalErrorDescription))
	}

	versionExists, err := api.dataStore.Backend.CheckVersionExistsStatic(ctx, datasetID, edition, versionNumber)
	if err != nil {
		log.Error(ctx, "createVersion endpoint: failed to check version existence", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(err, models.InternalError, models.InternalErrorDescription))
	}
	if versionExists {
		log.Error(ctx, "createVersion endpoint: version already exists", errs.ErrVersionAlreadyExists, logData)
		return nil, models.NewErrorResponse(http.StatusConflict, nil, models.NewValidationError(models.ErrVersionAlreadyExists, models.ErrVersionAlreadyExistsDescription))
	}

	createdVersion, err := api.dataStore.Backend.AddVersionStatic(ctx, newVersion)
	if err != nil {
		log.Error(ctx, "createVersion endpoint: failed to create version", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(err, models.InternalError, models.InternalErrorDescription))
	}

	createdVersionJSON, err := json.Marshal(createdVersion)
	if err != nil {
		log.Error(ctx, "createVersion endpoint: failed to marshal version to JSON", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(errs.ErrInternalServer, models.JSONMarshalError, models.InternalErrorDescription))
	}

	dpresponse.SetETag(w, createdVersion.ETag)

	return models.NewSuccessResponse(createdVersionJSON, http.StatusCreated, nil), nil
}
