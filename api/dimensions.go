package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/log.go/log"
	"github.com/globalsign/mgo/bson"
	"github.com/gorilla/mux"
)

func (api *DatasetAPI) getDimensions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version, "func": "getDimensions"}

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

		listOfDimensions := &models.DatasetDimensionResults{Items: results}

		b, err := json.Marshal(listOfDimensions)
		if err != nil {
			log.Event(ctx, "failed to marshal list of dimension resources into bytes", log.ERROR, log.Error(err), logData)
			return nil, err
		}
		return b, nil
	}()
	if err != nil {
		handleDimensionsErr(ctx, w, err, logData)
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

// getLimitAndOffset obtains the int values for limit and offset from the provided map
// with a default of 0 if not present.
func getLimitAndOffset(queryVars url.Values) (limit, offset int, err error) {
	limitStr, found := queryVars["limit"]
	if found {
		limit, err = strconv.Atoi(limitStr[0])
		if err != nil {
			return -1, -1, errs.ErrInvalidQueryParameter
		}
	} else {
		limit = 0
	}

	offsetStr, found := queryVars["offset"]
	if found {
		offset, err = strconv.Atoi(offsetStr[0])
		if err != nil {
			return -1, -1, errs.ErrInvalidQueryParameter
		}
	}
	if !found {
		offset = 0
	}
	return
}

func (api *DatasetAPI) getDimensionOptions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	versionID := vars["version"]
	dimension := vars["dimension"]

	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": versionID, "dimension": dimension, "func": "getDimensionOptions"}
	authorised := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	b, err := func() ([]byte, error) {

		limit, offset, err := getLimitAndOffset(r.URL.Query())
		if err != nil {
			log.Event(ctx, "failed to obtain limit and offest from request query paramters", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		version, err := api.dataStore.Backend.GetVersion(datasetID, edition, versionID, state)
		if err != nil {
			log.Event(ctx, "failed to get version", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		if err = models.CheckState("version", version.State); err != nil {
			logData["version_state"] = version.State
			log.Event(ctx, "unpublished version has an invalid state", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		totalCount, err := api.dataStore.Backend.CountDimensionOptions(version, dimension)
		if err != nil {
			log.Event(ctx, "failed to count dimension options", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		results, err := api.dataStore.Backend.GetDimensionOptions(version, dimension)
		if err != nil {
			log.Event(ctx, "failed to get a list of dimension options", log.ERROR, log.Error(err), logData)
			return nil, err
		}
		results.Count = len(results.Items)
		results.TotalCount = totalCount
		results.Limit = limit
		results.Offset = offset

		for i := range results.Items {
			results.Items[i].Links.Version.HRef = fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s",
				api.host, datasetID, edition, versionID)
			results.Items[i].Links.Version.ID = versionID
		}

		b, err := json.Marshal(results)
		if err != nil {
			log.Event(ctx, "failed to marshal list of dimension option resources into bytes", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		return b, nil
	}()
	if err != nil {
		handleDimensionsErr(ctx, w, err, logData)
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

func handleDimensionsErr(ctx context.Context, w http.ResponseWriter, err error, data log.Data) {
	if data == nil {
		data = log.Data{}
	}

	var status int
	response := err
	switch {
	case errs.BadRequestMap[err]:
		status = http.StatusBadRequest
	case errs.NotFoundMap[err]:
		status = http.StatusNotFound
	default:
		status = http.StatusInternalServerError
		response = errs.ErrInternalServer
	}

	data["response_status"] = status
	log.Event(ctx, "request unsuccessful", log.ERROR, log.Error(err), data)
	http.Error(w, response.Error(), status)
}
