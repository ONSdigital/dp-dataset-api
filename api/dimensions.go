package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/utils"
	"github.com/ONSdigital/log.go/log"
	"github.com/globalsign/mgo/bson"
	"github.com/gorilla/mux"
)

const maxIDs = 200

// MaxIDs returns the maximum number of IDs acceptable in a list
var MaxIDs = func() int {
	return maxIDs
}

func (api *DatasetAPI) getDimensions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version, "func": "getDimensions"}

	offsetParameter := r.URL.Query().Get("offset")
	limitParameter := r.URL.Query().Get("limit")

	offset := api.defaultOffset
	limit := api.defaultLimit
	var err error

	if offsetParameter != "" {
		logData["offset"] = offsetParameter
		offset, err = utils.ValidatePositiveInt(offsetParameter)
		if err != nil {
			log.Event(ctx, "invalid query parameter: offset", log.ERROR, log.Error(err), logData)
			handleDimensionsErr(ctx, w, "invalid query parameter: limit", err, logData)
			return
		}
	}

	if limitParameter != "" {
		logData["limit"] = limitParameter
		limit, err = utils.ValidatePositiveInt(limitParameter)
		if err != nil {
			log.Event(ctx, "invalid query parameter: limit", log.ERROR, log.Error(err), logData)
			handleDimensionsErr(ctx, w, "invalid query parameter: limit", err, logData)
			return
		}
	}

	if limit > api.maxLimit {
		logData["max_limit"] = api.maxLimit
		err = errs.ErrInvalidQueryParameter
		log.Event(ctx, "limit is greater than the maximum allowed", log.ERROR, logData)
		handleDimensionsErr(ctx, w, "unpublished version has an invalid state", err, logData)
		return
	}

	b, err := func() ([]byte, error) {
		authorised := api.authenticate(r, logData)

		var state string
		if !authorised {
			state = models.PublishedState
		}

		versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, state)
		if err != nil {
			log.Event(ctx, "datastore.getversion returned an error", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		if err = models.CheckState("version", versionDoc.State); err != nil {
			logData["state"] = versionDoc.State
			log.Event(ctx, "unpublished version has an invalid state", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		dimensions, err := api.dataStore.Backend.GetDimensions(datasetID, versionDoc.ID)
		if err != nil {
			log.Event(ctx, "failed to get version dimensions", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		results, err := api.createListOfDimensions(versionDoc, dimensions)
		if err != nil {
			log.Event(ctx, "failed to convert bson to dimension", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		sort.Slice(results, func(i, j int) bool {
			return results[i].Name < results[j].Name
		})

		slicedResults := utils.Slice(results, offset, limit)

		listOfDimensions := &models.DatasetDimensionResults{
			Items:      slicedResults,
			Offset:     offset,
			Limit:      limit,
			Count:      len(slicedResults),
			TotalCount: len(dimensions),
		}

		b, err := json.Marshal(listOfDimensions)
		if err != nil {
			log.Event(ctx, "failed to marshal list of dimension resources into bytes", log.ERROR, log.Error(err), logData)
			return nil, err
		}
		return b, nil
	}()
	if err != nil {
		handleDimensionsErr(ctx, w, "", err, logData)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Event(ctx, "error writing bytes to response", log.ERROR, log.Error(err), logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Event(ctx, "getDimensions endpoint: request successful", log.INFO, logData)
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

	bson.Unmarshal(b, &dim)

	return &dim, nil
}

func (api *DatasetAPI) getDimensionOptions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	versionID := vars["version"]
	dimension := vars["dimension"]
	offsetParameter := r.URL.Query().Get("offset")
	limitParameter := r.URL.Query().Get("limit")

	offset := api.defaultOffset
	limit := api.defaultLimit

	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": versionID, "dimension": dimension, "func": "getDimensionOptions"}
	authorised := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	// get list of option IDs that we want to get
	ids, err := utils.GetQueryParamListValues(r.URL.Query(), "id", MaxIDs())
	if err != nil {
		logData["query_params"] = r.URL.RawQuery
		handleDimensionsErr(ctx, w, "failed to obtain list of IDs from request query parameters", err, logData)
		return

	}

	if offsetParameter != "" {
		logData["offset"] = offsetParameter
		offset, err = utils.ValidatePositiveInt(offsetParameter)
		if err != nil {
			log.Event(ctx, "invalid query parameter: offset", log.ERROR, log.Error(err), logData)
			handleDimensionsErr(ctx, w, "invalid query parameter: limit", err, logData)
			return
		}
	}

	if limitParameter != "" {
		logData["limit"] = limitParameter
		limit, err = utils.ValidatePositiveInt(limitParameter)
		if err != nil {
			log.Event(ctx, "invalid query parameter: limit", log.ERROR, log.Error(err), logData)
			handleDimensionsErr(ctx, w, "invalid query parameter: limit", err, logData)
			return
		}
	}

	if limit > api.maxLimit {
		logData["max_limit"] = api.maxLimit
		err = errs.ErrInvalidQueryParameter
		log.Event(ctx, "limit is greater than the maximum allowed", log.ERROR, logData)
		handleDimensionsErr(ctx, w, "unpublished version has an invalid state", err, logData)
		return
	}

	// ger version for provided dataset, edition and versionID
	version, err := api.dataStore.Backend.GetVersion(datasetID, edition, versionID, state)
	if err != nil {
		handleDimensionsErr(ctx, w, "failed to get version", err, logData)
		return
	}

	// vaidate state
	if err = models.CheckState("version", version.State); err != nil {
		logData["version_state"] = version.State
		handleDimensionsErr(ctx, w, "unpublished version has an invalid state", err, logData)
		return
	}

	var results *models.DimensionOptionResults
	if len(ids) == 0 {
		// get sorted dimension options, starting at offset index, with a limit on the number of items
		results, err = api.dataStore.Backend.GetDimensionOptions(version, dimension, offset, limit)
		if err != nil {
			handleDimensionsErr(ctx, w, "failed to get a list of dimension options", err, logData)
			return
		}
	} else {
		// get dimension options from the provided list of IDs, sorted by option
		results, err = api.dataStore.Backend.GetDimensionOptionsFromIDs(version, dimension, ids)
		if err != nil {
			handleDimensionsErr(ctx, w, "failed to get a list of dimension options", err, logData)
			return
		}
	}

	// populate links
	versionHref := fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s", api.host, datasetID, edition, versionID)
	for i := range results.Items {
		results.Items[i].Links.Version.HRef = versionHref
		results.Items[i].Links.Version.ID = versionID
	}

	b, err := json.Marshal(results)
	if err != nil {
		handleDimensionsErr(ctx, w, "failed to marshal list of dimension option resources into bytes", err, logData)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Event(ctx, "error writing bytes to response", log.ERROR, log.Error(err), logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Event(ctx, "get dimension options", log.INFO, logData)
}

// handleDimensionsErr maps the provided error to its corresponding status code.
// The error is logged with ERROR severity, but the stack trace is only shown for errors corresponding to InternalServerError status
func handleDimensionsErr(ctx context.Context, w http.ResponseWriter, msg string, err error, data log.Data) {
	if data == nil {
		data = log.Data{}
	}

	switch {
	case errs.BadRequestMap[err]:
		data["response_status"] = http.StatusBadRequest
		data["user_error"] = err.Error()
		log.Event(ctx, fmt.Sprintf("request unsuccessful: %s", msg), log.ERROR, data)
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errs.NotFoundMap[err]:
		data["response_status"] = http.StatusNotFound
		data["user_error"] = err.Error()
		log.Event(ctx, fmt.Sprintf("request unsuccessful: %s", msg), log.ERROR, data)
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		// a stack trace is added for Non User errors
		data["response_status"] = http.StatusInternalServerError
		log.Event(ctx, fmt.Sprintf("request unsuccessful: %s", msg), log.ERROR, log.Error(err), data)
		http.Error(w, errs.ErrInternalServer.Error(), http.StatusInternalServerError)
	}

}
