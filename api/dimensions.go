package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	"gopkg.in/mgo.v2/bson"
)

func (api *DatasetAPI) getDimensions(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	authorised, logData := api.authenticate(r, logData)

	var state string
	if !authorised {
		state = models.PublishedState
	}

	versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, state)
	if err != nil {
		log.ErrorC("failed to get version", err, logData)
		handleErrorType(dimensionDocType, err, w)
		return
	}

	if err = models.CheckState("version", versionDoc.State); err != nil {
		log.ErrorC("unpublished version has an invalid state", err, log.Data{"state": versionDoc.State})
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dimensions, err := api.dataStore.Backend.GetDimensions(datasetID, versionDoc.ID)
	if err != nil {
		log.ErrorC("failed to get version dimensions", err, logData)
		handleErrorType(dimensionDocType, err, w)
		return
	}

	results, err := api.createListOfDimensions(versionDoc, dimensions)
	if err != nil {
		log.ErrorC("failed to convert bson to dimension", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	listOfDimensions := &models.DatasetDimensionResults{Items: results}

	b, err := json.Marshal(listOfDimensions)
	if err != nil {
		log.ErrorC("failed to marshal list of dimension resources into bytes", err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	setJSONContentType(w)
	_, err = w.Write(b)
	if err != nil {
		log.Error(err, logData)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	log.Debug("get dimensions", log.Data{"dataset_id": datasetID, "edition": edition, "version": version})
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
		dimension.Links.Version = *versionDoc.Links.Self

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
