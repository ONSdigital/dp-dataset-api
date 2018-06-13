package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gedge/mgo/bson"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

func (api *DatasetAPI) getDimensions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}
	auditParams := common.Params{"dataset_id": datasetID, "edition": edition, "version": version}

	if err := api.auditor.Record(ctx, getDimensionsAction, audit.Attempted, auditParams); err != nil {
		auditActionFailure(ctx, getDimensionsAction, audit.Attempted, err, logData)
		handleDimensionsErr(ctx, w, err)
		return
	}

	b, err := func() ([]byte, error) {
		authorised, logData := api.authenticate(r, logData)

		var state string
		if !authorised {
			state = models.PublishedState
		}

		versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, state)
		if err != nil {
			logError(ctx, errors.WithMessage(err, "getDimensions endpoint: datastore.getversion returned an error"), logData)
			return nil, err
		}

		if err = models.CheckState("version", versionDoc.State); err != nil {
			logData["state"] = versionDoc.State
			logError(ctx, errors.WithMessage(err, "getDimensions endpoint: unpublished version has an invalid state"), logData)
			return nil, err
		}

		dimensions, err := api.dataStore.Backend.GetDimensions(datasetID, versionDoc.ID)
		if err != nil {
			logError(ctx, errors.WithMessage(err, "getDimensions endpoint: failed to get version dimensions"), logData)
			return nil, err
		}

		results, err := api.createListOfDimensions(versionDoc, dimensions)
		if err != nil {
			logError(ctx, errors.WithMessage(err, "getDimensions endpoint: failed to convert bson to dimension"), logData)
			return nil, err
		}

		listOfDimensions := &models.DatasetDimensionResults{Items: results}

		b, err := json.Marshal(listOfDimensions)
		if err != nil {
			logError(ctx, errors.WithMessage(err, "getDimensions endpoint: failed to marshal list of dimension resources into bytes"), logData)
			return nil, err
		}
		return b, nil
	}()

	if err != nil {
		if auditErr := api.auditor.Record(ctx, getDimensionsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			auditActionFailure(ctx, getDimensionsAction, audit.Unsuccessful, auditErr, logData)
		}
		handleDimensionsErr(ctx, w, err)
		return
	}

	if auditErr := api.auditor.Record(ctx, getDimensionsAction, audit.Successful, auditParams); auditErr != nil {
		auditActionFailure(ctx, getDimensionsAction, audit.Successful, auditErr, logData)
		handleDimensionsErr(ctx, w, auditErr)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		logError(ctx, errors.WithMessage(err, "getDimensions endpoint: error writing bytes to response"), logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	logInfo(ctx, "getDimensions endpoint: request successful", logData)
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
	vars := mux.Vars(r)
	datasetID := vars["id"]
	editionID := vars["edition"]
	versionID := vars["version"]
	dimension := vars["dimension"]

	logData := log.Data{"dataset_id": datasetID, "edition": editionID, "version": versionID, "dimension": dimension}

	authorised, logData := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	version, err := api.dataStore.Backend.GetVersion(datasetID, editionID, versionID, state)
	if err != nil {
		log.ErrorC("failed to get version", err, logData)
		handleErrorType(versionDocType, err, w)
		return
	}

	if err = models.CheckState("version", version.State); err != nil {
		log.ErrorC("unpublished version has an invalid state", err, log.Data{"state": version.State})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	results, err := api.dataStore.Backend.GetDimensionOptions(version, dimension)
	if err != nil {
		log.ErrorC("failed to get a list of dimension options", err, logData)
		handleErrorType(dimensionOptionDocType, err, w)
		return
	}

	for i := range results.Items {
		results.Items[i].Links.Version.HRef = fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s",
			api.host, datasetID, editionID, versionID)
	}

	b, err := json.Marshal(results)
	if err != nil {
		log.ErrorC("failed to marshal list of dimension option resources into bytes", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Debug("get dimension options", logData)
}

func handleDimensionsErr(ctx context.Context, w http.ResponseWriter, err error) {
	var responseStatus int

	switch {
	case err == errs.ErrDatasetNotFound:
		responseStatus = http.StatusNotFound
	case err == errs.ErrEditionNotFound:
		responseStatus = http.StatusNotFound
	case err == errs.ErrVersionNotFound:
		responseStatus = http.StatusNotFound
	case err == errs.ErrDimensionsNotFound:
		responseStatus = http.StatusNotFound
	default:
		responseStatus = http.StatusInternalServerError
	}

	logError(ctx, errors.WithMessage(err, "request unsuccessful"), log.Data{"responseStatus": responseStatus})
	http.Error(w, err.Error(), responseStatus)
}
