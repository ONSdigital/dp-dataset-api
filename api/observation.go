package api

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-filter/observation"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
)

//go:generate moq -out ../mocks/observation_store.go -pkg mocks . ObservationStore

const defaultObservationLimit = 2

func errorIncorrectQueryParameters(params []string) error {
	return fmt.Errorf("Incorrect selection of query parameters: %v, these dimensions do not exist for this version of the dataset", params)
}

func errorMissingQueryParameters(params []string) error {
	return fmt.Errorf("Missing query parameters for the following dimensions: %v", params)
}

// ObservationStore provides filtered observation data in CSV rows.
type ObservationStore interface {
	GetCSVRows(filter *observation.Filter, limit *int) (observation.CSVRowReader, error)
}

func (api *DatasetAPI) getObservations(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	datasetID := vars["id"]
	edition := vars["edition"]
	version := vars["version"]

	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	// get dataset document
	datasetDoc, err := api.dataStore.Backend.GetDataset(datasetID)
	if err != nil {
		log.Error(err, logData)
		handleObservationsErrorType(w, err)
		return
	}

	authorised, logData := api.authenticate(r, logData)

	var state string

	// if request is authenticated then access resources of state other than published
	if !authorised {
		// Check for current sub document
		if datasetDoc.Current == nil || datasetDoc.Current.State != models.PublishedState {
			logData["dataset_doc"] = datasetDoc.Current
			log.ErrorC("found dataset but currently unpublished", errs.ErrDatasetNotFound, logData)
			http.Error(w, errs.ErrDatasetNotFound.Error(), http.StatusNotFound)
			return
		}

		state = datasetDoc.Current.State
	}

	if err = api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
		log.ErrorC("failed to find edition for dataset", err, logData)
		handleObservationsErrorType(w, err)
		return
	}

	versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, state)
	if err != nil {
		log.ErrorC("failed to find version for dataset edition", err, logData)
		handleObservationsErrorType(w, err)
		return
	}

	if err = models.CheckState("version", versionDoc.State); err != nil {
		logData["state"] = versionDoc.State
		log.ErrorC("unpublished version has an invalid state", err, logData)
		handleObservationsErrorType(w, err)
		return
	}

	if versionDoc.Headers == nil {
		logData["version_doc"] = versionDoc
		log.Error(errs.ErrMissingVersionHeaders, logData)
		http.Error(w, errs.ErrMissingVersionHeaders.Error(), http.StatusInternalServerError)
		return
	}

	// loop through version headers to retrieve list of dimension options
	validDimensionOptions, dimensionOffset, err := getListOfValidDimensionOptions(versionDoc.Headers)
	if err != nil {
		log.ErrorC("unable to distinguish headers from version document", err, logData)
		handleObservationsErrorType(w, err)
		return
	}
	logData["dimension_options"] = validDimensionOptions

	// check query parameters match the version headers
	queryParameters, err := extractQueryParameters(r, validDimensionOptions)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	logData["query_parameters"] = queryParameters

	// use default limit of 2 to set the number of observations that can be returned
	// this will allow the response from neo4j to be returned faster if more than 1
	// observation is found for query and yet dataset API can understand that the request
	// has not selected enough dimensions to find a single observation
	observationLimit := defaultObservationLimit

	// retrieve observations (for now it will only ever return a maximum of 1 observation)
	headerRow, observationRow, err := api.getObservationList(versionDoc.ID, queryParameters, observationLimit)
	if err != nil {
		log.ErrorC("unable to find a single observation", err, logData)
		handleObservationsErrorType(w, err)
		return
	}

	observationDoc := models.CreateObservationDoc(r.URL.RawQuery, versionDoc, headerRow, observationRow, dimensionOffset, queryParameters)

	setJSONContentType(w)

	// The ampersand "&" is escaped to "\u0026" to keep some browsers from
	// misinterpreting JSON output as HTML. This escaping can be disabled using
	// an Encoder that had SetEscapeHTML(false) called on it.
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	if err = enc.Encode(observationDoc); err != nil {
		log.ErrorC("failed to marshal metadata resource into bytes", err, logData)
		handleObservationsErrorType(w, err)
		return
	}

	log.Debug("get single observation relative to a selected set of dimension options for a version", logData)
}

func getListOfValidDimensionOptions(headerRow []string) ([]string, int, error) {
	metaData := strings.Split(headerRow[0], "_")

	if len(metaData) < 2 {
		return nil, 0, errs.ErrIndexOutOfRange
	}

	dimensionOffset, err := strconv.Atoi(metaData[1])
	if err != nil {
		return nil, 0, err
	}

	var headers []string
	for i := dimensionOffset + 2; i <= len(headerRow); i += 2 {
		headers = append(headers, headerRow[i])
	}

	return headers, dimensionOffset, nil
}

func extractQueryParameters(r *http.Request, validDimensions []string) (map[string]string, error) {
	queryParameters := make(map[string]string)
	var incorrectQueryParameters, missingQueryParameters []string

	urlValues := r.URL.Query()

	// Determine if any request query parameters are invalid dimensions
	// and map the dimensions with their equivalent values in map
	for dimension, option := range urlValues {
		queryParamExists := false
		for _, validDimension := range validDimensions {
			if dimension == validDimension {
				queryParamExists = true
				queryParameters[dimension] = option[0]
				break
			}
		}
		if !queryParamExists {
			incorrectQueryParameters = append(incorrectQueryParameters, dimension)
		}
	}

	if len(incorrectQueryParameters) > 0 {
		return nil, errorIncorrectQueryParameters(incorrectQueryParameters)
	}

	// Determine if any dimensions have not been set in request query parameters
	if len(queryParameters) != len(validDimensions) {
		for _, validDimension := range validDimensions {
			if queryParameters[validDimension] == "" {
				missingQueryParameters = append(missingQueryParameters, validDimension)
			}
		}
		return nil, errorMissingQueryParameters(missingQueryParameters)
	}

	return queryParameters, nil
}

func (api *DatasetAPI) getObservationList(instanceID string, queryParamters map[string]string, limit int) ([]string, []string, error) {

	// Build query (observation.Filter type)
	var dimensionFilters []*observation.DimensionFilter

	for dimension, option := range queryParamters {
		dimensionFilter := &observation.DimensionFilter{
			Name:    dimension,
			Options: []string{option},
		}

		dimensionFilters = append(dimensionFilters, dimensionFilter)
	}

	query := observation.Filter{
		InstanceID:       instanceID,
		DimensionFilters: dimensionFilters,
	}

	csvRowReader, err := api.observationStore.GetCSVRows(&query, &limit)
	if err != nil {
		return nil, nil, err
	}

	observationCount := 0

	headerRow, err := csvRowReader.Read()
	if err != nil {
		return nil, nil, err
	}
	defer csvRowReader.Close()

	headerRowReader := csv.NewReader(strings.NewReader(headerRow))
	headerRowArray, err := headerRowReader.Read()
	if err != nil {
		return nil, nil, err
	}

	var observationRow string
	var observationRows []string
	// Iterate over observation row reader
	for {
		observationRow, err = csvRowReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		// only ever return one observation otherwise error
		observationCount++

		if observationCount > 1 {
			return nil, nil, errs.ErrMoreThanOneObservationFound
		}

		observationRows = append(observationRows, observationRow)
	}

	observationRowReader := csv.NewReader(strings.NewReader(observationRows[0]))
	observationRowArray, err := observationRowReader.Read()
	if err != nil {
		return nil, nil, err
	}

	return headerRowArray, observationRowArray, nil
}

func handleObservationsErrorType(w http.ResponseWriter, err error) {
	log.Error(err, nil)

	switch err {
	case errs.ErrDatasetNotFound:
		http.Error(w, err.Error(), http.StatusNotFound)
	case errs.ErrEditionNotFound:
		http.Error(w, err.Error(), http.StatusNotFound)
	case errs.ErrVersionNotFound:
		http.Error(w, err.Error(), http.StatusNotFound)
	case errs.ErrObservationsNotFound:
		http.Error(w, err.Error(), http.StatusNotFound)
	case errs.ErrMoreThanOneObservationFound:
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
