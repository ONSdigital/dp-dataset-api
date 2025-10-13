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
		} else {
			log.Error(ctx, "getDatasetEditions endpoint: failed to get versions", err, logData)
			handleVersionAPIErr(ctx, errs.ErrInternalServer, w, logData)
			return nil, 0, errs.ErrInternalServer
		}
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
			} else {
				log.Error(ctx, "getDatasetEditions endpoint: failed to get dataset", err, logData)
				handleVersionAPIErr(ctx, errs.ErrInternalServer, w, logData)
				return nil, 0, errs.ErrInternalServer
			}
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
func (api *DatasetAPI) addDatasetVersionCondensed(ctx context.Context, w http.ResponseWriter, r *http.Request) (*models.SuccessResponse, *models.ErrorResponse) {
	defer dphttp.DrainBody(r)

	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition}

	log.Info(ctx, "condensed endpoint called", logData)

	versionRequest := &models.Version{}
	if err := json.NewDecoder(r.Body).Decode(versionRequest); err != nil {
		log.Error(ctx, "failed to unmarshal version", err, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewError(ctx, err, models.JSONUnmarshalError, "failed to unmarshal version"))
	}

	if missingFields := validateVersionFields(versionRequest); len(missingFields) > 0 {
		log.Error(ctx, "failed validation check for version update", fmt.Errorf("missing mandatory version fields"), logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewValidationError(ctx, models.ErrMissingParameters, models.ErrMissingParametersDescription+" "+strings.Join(missingFields, " ")))
	}

	// validate versiontype
	if versionRequest.Type != "" && versionRequest.Type != models.Static.String() {
		log.Error(ctx, "addDatasetVersionCondensed endpoint: only allowed to create static type versions", errs.ErrInvalidQueryParameter, logData)
		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewValidationError(ctx, models.ErrInvalidTypeError, models.ErrInvalidType))
	}

	// Validate dataset existence
	if err := api.dataStore.Backend.CheckDatasetExists(ctx, datasetID, ""); err != nil {
		log.Error(ctx, "failed to find dataset", err, logData)
		return nil, models.NewErrorResponse(http.StatusNotFound, nil, models.NewValidationError(ctx, models.ErrDatasetNotFound, models.ErrDatasetNotFoundDescription))
	}

	latestVersion, err := api.dataStore.Backend.GetLatestVersionStatic(ctx, datasetID, edition, "")

	if err != nil && !errors.Is(err, errs.ErrVersionNotFound) {
		log.Error(ctx, "failed to check latest version", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(ctx, err, "failed to check latest version", "internal error"))
	}

	var nextVersion int
	versionRequest.Edition = edition

	if errors.Is(err, errs.ErrVersionNotFound) {
		log.Warn(ctx, "edition not found, defaulting to version 1", logData)
		nextVersion = 1
	} else {
		nextVersion = latestVersion.Version + 1
	}

	if err == nil && latestVersion.State != models.PublishedState {
		log.Error(ctx, "unpublished version already exists", errors.New("cannot create new version when unpublished version exists"),
			log.Data{"state": latestVersion.State, "version": latestVersion.Version})

		return nil, models.NewErrorResponse(http.StatusBadRequest, nil, models.NewError(ctx, err, models.ErrVersionAlreadyExists, models.ErrVersionAlreadyExistsDescription+" - unpublished_version: "+strconv.Itoa(latestVersion.Version)))
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
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(ctx, err, "failed to add version", "internal error"))
	}

	datasetDoc, err := api.dataStore.Backend.GetDataset(ctx, datasetID)
	if err != nil {
		log.Error(ctx, "failed to get dataset", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(ctx, err, "failed to get dataset", "internal error"))
	}

	datasetDoc.Next.LastUpdated = newVersion.LastUpdated
	datasetDoc.Next.State = models.AssociatedState

	if err := api.dataStore.Backend.UpsertDataset(ctx, datasetID, datasetDoc); err != nil {
		log.Error(ctx, "failed to update dataset", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(ctx, err, "failed to update dataset", "internal error"))
	}

	log.Info(ctx, "add version: request successful", logData)

	dpresponse.SetETag(w, newVersion.ETag)

	response, err := json.Marshal(newVersion)
	if err != nil {
		log.Error(ctx, "failed to marshal version to JSON", err, logData)
		return nil, models.NewErrorResponse(http.StatusInternalServerError, nil, models.NewError(ctx, errs.ErrInternalServer, "failed to marshal version to JSON", "internal error"))
	}

	headers := map[string]string{
		"Code": strconv.Itoa(http.StatusCreated),
	}
	return models.NewSuccessResponse(response, http.StatusCreated, headers), nil
}
