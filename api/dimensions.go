package api

import (
	"context"
	"fmt"
	"net/http"
	"sort"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/utils"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
)

const maxIDs = 200

// MaxIDs returns the maximum number of IDs acceptable in a list
var MaxIDs = func() int {
	return maxIDs
}

// getDimensions returns a list of dimensions, the total count of dimensions that match the query parameters and an error
func (api *DatasetAPI) getDimensions(w http.ResponseWriter, r *http.Request, limit, offset int) (dimensionsList interface{}, totalCount int, err error) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version, "func": "getDimensions"}

	versionNumber, err := models.ParseAndValidateVersionNumber(ctx, version)
	if err != nil {
		handleDimensionsErr(ctx, w, "invalid version request", err, logData)
		return nil, 0, err
	}

	list, totalCount, err := func() ([]models.Dimension, int, error) {
		authorised := api.authenticate(r, logData)

		var state string
		if !authorised {
			state = models.PublishedState
		}

		versionDoc, err := api.dataStore.Backend.GetVersion(ctx, datasetID, edition, versionNumber, state)
		if err != nil {
			log.Error(ctx, "datastore.getversion returned an error", err, logData)
			return nil, 0, err
		}

		if err = models.CheckState("version", versionDoc.State); err != nil {
			logData["state"] = versionDoc.State
			log.Error(ctx, "unpublished version has an invalid state", err, logData)
			return nil, 0, err
		}

		dimensions, err := api.dataStore.Backend.GetDimensions(ctx, versionDoc.ID)
		if err != nil {
			log.Error(ctx, "failed to get version dimensions", err, logData)
			return nil, 0, err
		}

		slicedResults := []models.Dimension{}

		if limit > 0 {
			results, err := api.createListOfDimensions(versionDoc, dimensions)
			if err != nil {
				log.Error(ctx, "failed to convert bson to dimension", err, logData)
				return nil, 0, err
			}

			sort.Slice(results, func(i, j int) bool {
				return results[i].Name < results[j].Name
			})

			slicedResults = utils.Slice(results, offset, limit)
		}

		return slicedResults, len(dimensions), nil
	}()
	if err != nil {
		handleDimensionsErr(ctx, w, "", err, logData)
		return nil, 0, err
	}
	return list, totalCount, nil
}

func (api *DatasetAPI) createListOfDimensions(versionDoc *models.Version, dimensions []bson.M) ([]models.Dimension, error) {
	// Get dimension description from the version document and add to hash map
	dimensionDescriptions := make(map[string]string)
	dimensionLabels := make(map[string]string)
	for i := range versionDoc.Dimensions {
		details := &versionDoc.Dimensions[i]
		dimensionDescriptions[details.Name] = details.Description
		dimensionLabels[details.Name] = details.Label
	}

	results := make([]models.Dimension, 0, len(dimensions))
	for _, dim := range dimensions {
		opt, err := convertBSONToDimensionOption(dim["doc"])
		if err != nil {
			return nil, err
		}

		dimension := models.Dimension{Name: opt.Name}
		dimension.Links.CodeList = opt.Links.CodeList
		dimension.Links.Options = models.LinkObject{ID: opt.Name, HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s/dimensions/%s/options",
			api.host, versionDoc.Links.Dataset.ID, versionDoc.Edition, versionDoc.Links.Version.ID, opt.Name)}
		dimension.Links.Version = models.LinkObject{HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s",
			api.host, versionDoc.Links.Dataset.ID, versionDoc.Edition, versionDoc.Links.Version.ID)}

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

	if err := bson.Unmarshal(b, &dim); err != nil {
		return nil, err
	}

	return &dim, nil
}

// getDimensionOptions returns a list of options, the total count of options that match the query parameters and an error
//
// TODO: Refactor this to have named results
//
//nolint:gocritic
func (api *DatasetAPI) getDimensionOptions(w http.ResponseWriter, r *http.Request, limit, offset int) (interface{}, int, error) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	versionID := vars["version"]
	dimension := vars["dimension"]

	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": versionID, "dimension": dimension, "func": "getDimensionOptions"}
	authorised := api.authenticate(r, logData)

	versionName, err := models.ParseAndValidateVersionNumber(ctx, versionID)
	if err != nil {
		log.Error(ctx, "invalid version requested", err, logData)
		handleDimensionsErr(ctx, w, "invalid version", err, logData)
		return nil, 0, err
	}

	var state string
	if !authorised {
		state = models.PublishedState
	}

	// get list of option IDs that we want to get
	ids, err := utils.GetQueryParamListValues(r.URL.Query(), "id", MaxIDs())
	if err != nil {
		logData["query_params"] = r.URL.RawQuery
		handleDimensionsErr(ctx, w, "failed to obtain list of IDs from request query parameters", err, logData)
		return nil, 0, err
	}

	// ger version for provided dataset, edition and versionID
	version, err := api.dataStore.Backend.GetVersion(ctx, datasetID, edition, versionName, state)
	if err != nil {
		handleDimensionsErr(ctx, w, "failed to get version", err, logData)
		return nil, 0, err
	}

	// vaidate state
	if err = models.CheckState("version", version.State); err != nil {
		logData["version_state"] = version.State
		handleDimensionsErr(ctx, w, "unpublished version has an invalid state", err, logData)
		return nil, 0, err
	}

	var results []*models.PublicDimensionOption
	var totalCount int
	if len(ids) == 0 {
		// get sorted dimension options, starting at offset index, with a limit on the number of items
		results, totalCount, err = api.dataStore.Backend.GetDimensionOptions(ctx, version, dimension, offset, limit)
		if err != nil {
			handleDimensionsErr(ctx, w, "failed to get a list of dimension options", err, logData)
			return nil, 0, err
		}
	} else {
		// get dimension options from the provided list of IDs, sorted by option
		results, totalCount, err = api.dataStore.Backend.GetDimensionOptionsFromIDs(ctx, version, dimension, ids)
		if err != nil {
			handleDimensionsErr(ctx, w, "failed to get a list of dimension options", err, logData)
			return nil, 0, err
		}
	}

	// populate links
	versionHref := fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s", api.host, datasetID, edition, versionID)
	for i := range results {
		results[i].Links.Version.HRef = versionHref
		results[i].Links.Version.ID = versionID
	}

	return results, totalCount, nil
}

// handleDimensionsErr maps the provided error to its corresponding status code.
// The error is logged with ERROR severity, but the stack trace is only shown for errors corresponding to InternalServerError status
func handleDimensionsErr(ctx context.Context, w http.ResponseWriter, msg string, err error, data log.Data) {
	if data == nil {
		data = log.Data{}
	}

	// Switch by error type
	switch err.(type) {
	case errs.ErrInvalidPatch:
		data["response_status"] = http.StatusBadRequest
		data["user_error"] = err.Error()
		log.Error(ctx, fmt.Sprintf("request unsuccessful: %s", msg), err, data)
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		// Switch by error message
		switch {
		case errs.BadRequestMap[err]:
			data["response_status"] = http.StatusBadRequest
			data["user_error"] = err.Error()
			log.Error(ctx, fmt.Sprintf("request unsuccessful: %s", msg), err, data)
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errs.NotFoundMap[err]:
			data["response_status"] = http.StatusNotFound
			data["user_error"] = err.Error()
			log.Error(ctx, fmt.Sprintf("request unsuccessful: %s", msg), err, data)
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			// a stack trace is added for Non User errors
			data["response_status"] = http.StatusInternalServerError
			log.Error(ctx, fmt.Sprintf("request unsuccessful: %s", msg), err, data)
			http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
		}
	}
}
