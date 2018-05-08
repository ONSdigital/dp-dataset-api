package api

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-filter/observation"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
)

//go:generate moq -out ../mocks/observation_store.go -pkg mocks . ObservationStore

// Upper limit, if this is not big enough, we may need to consider increasing value
// and then if this has a performance hit then consider paging
const (
	defaultObservationLimit = 10000
	defaultOffset           = 0
)

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

	var (
		state   string
		dataset *models.Dataset
	)

	// if request is authenticated then access resources of state other than published
	if !authorised {
		// Check for current sub document
		if datasetDoc.Current == nil || datasetDoc.Current.State != models.PublishedState {
			logData["dataset_doc"] = datasetDoc.Current
			log.ErrorC("found dataset but currently unpublished", errs.ErrDatasetNotFound, logData)
			http.Error(w, errs.ErrDatasetNotFound.Error(), http.StatusNotFound)
			return
		}

		dataset = datasetDoc.Current
		state = dataset.State
	} else {
		dataset = datasetDoc.Next
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
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	// loop through version headers to retrieve list of dimension names
	validDimensionNames, dimensionOffset, err := getListOfValidDimensionNames(versionDoc.Headers)
	if err != nil {
		log.ErrorC("unable to distinguish headers from version document", err, logData)
		handleObservationsErrorType(w, err)
		return
	}
	logData["version_dimensions"] = validDimensionNames

	// check query parameters match the version headers
	queryParameters, err := extractQueryParameters(r.URL.Query(), validDimensionNames)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	logData["query_parameters"] = queryParameters
	log.Info("correctly identified query parameters", logData)

	// retrieve observations
	observations, err := api.getObservationList(versionDoc, queryParameters, defaultObservationLimit, dimensionOffset, logData)
	if err != nil {
		log.ErrorC("unable to retrieve observations", err, logData)
		handleObservationsErrorType(w, err)
		return
	}

	observationsDoc := models.CreateObservationsDoc(r.URL.RawQuery, versionDoc, dataset, observations, queryParameters, defaultOffset, defaultObservationLimit)

	setJSONContentType(w)

	// The ampersand "&" is escaped to "\u0026" to keep some browsers from
	// misinterpreting JSON output as HTML. This escaping can be disabled using
	// an Encoder that had SetEscapeHTML(false) called on it.
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	if err = enc.Encode(observationsDoc); err != nil {
		log.ErrorC("failed to marshal metadata resource into bytes", err, logData)
		handleObservationsErrorType(w, err)
		return
	}

	log.Info("successfully retrieved observations relative to a selected set of dimension options for a version", logData)
}

func getListOfValidDimensionNames(headerRow []string) ([]string, int, error) {
	metaData := strings.Split(headerRow[0], "_")

	if len(metaData) < 2 {
		return nil, 0, errs.ErrIndexOutOfRange
	}

	dimensionOffset, err := strconv.Atoi(metaData[1])
	if err != nil {
		return nil, 0, err
	}

	var dimensionNames []string
	for i := dimensionOffset + 2; i <= len(headerRow); i += 2 {
		dimensionNames = append(dimensionNames, headerRow[i])
	}

	return dimensionNames, dimensionOffset, nil
}

func extractQueryParameters(urlQuery url.Values, validDimensions []string) (map[string]string, error) {
	queryParameters := make(map[string]string)
	var incorrectQueryParameters, missingQueryParameters []string

	// Determine if any request query parameters are invalid dimensions
	// and map the valid dimensions with their equivalent values in map
	for dimension, option := range urlQuery {
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

func (api *DatasetAPI) getObservationList(versionDoc *models.Version, queryParamters map[string]string, limit, dimensionOffset int, logData log.Data) ([]models.Observation, error) {

	// Build query (observation.Filter type)
	var dimensionFilters []*observation.DimensionFilter

	// Unable to have more than one wildcard per query parameter
	var wildcards int
	var wildcardParameter string

	// Build dimension filter object to create queryObject for neo4j
	for dimension, option := range queryParamters {
		if option == "*" {
			wildcardParameter = dimension
			wildcards++
			continue
		}

		dimensionFilter := &observation.DimensionFilter{
			Name:    dimension,
			Options: []string{option},
		}

		dimensionFilters = append(dimensionFilters, dimensionFilter)
	}

	if wildcards > 1 {
		return nil, errs.ErrTooManyWildcards
	}

	queryObject := observation.Filter{
		InstanceID:       versionDoc.ID,
		DimensionFilters: dimensionFilters,
	}
	logData["query_object"] = queryObject

	log.Info("query object built to retrieve observations from neo4j", logData)

	csvRowReader, err := api.observationStore.GetCSVRows(&queryObject, &limit)
	if err != nil {
		return nil, err
	}

	headerRow, err := csvRowReader.Read()
	if err != nil {

		return nil, err
	}
	defer csvRowReader.Close()

	headerRowReader := csv.NewReader(strings.NewReader(headerRow))
	headerRowArray, err := headerRowReader.Read()
	if err != nil {
		return nil, err
	}

	var observationRow string
	var observations []models.Observation
	// Iterate over observation row reader
	for {
		observationRow, err = csvRowReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			if strings.Contains(err.Error(), "the filter options created no results") {
				return nil, errs.ErrObservationsNotFound
			}
			return nil, err
		}

		observationRowReader := csv.NewReader(strings.NewReader(observationRow))
		observationRowArray, err := observationRowReader.Read()
		if err != nil {
			return nil, err
		}

		// TODO for the below maybe put this in a seperate function?
		observation := models.Observation{
			Observation: observationRowArray[0],
		}

		// add observation metadata
		if dimensionOffset != 0 {
			observationMetaData := make(map[string]string)

			for i := 1; i < dimensionOffset+1; i++ {
				observationMetaData[headerRowArray[i]] = observationRowArray[i]
			}

			observation.Metadata = observationMetaData
		}

		if wildcardParameter != "" {
			dimensions := make(map[string]*models.DimensionObject)

			for i := dimensionOffset + 1; i < len(observationRowArray); i++ {

				if strings.ToLower(headerRowArray[i]) == wildcardParameter {
					for _, versionDimension := range versionDoc.Dimensions {
						if versionDimension.Name == wildcardParameter {

							dimensions[headerRowArray[i]] = &models.DimensionObject{
								ID:    observationRowArray[i-1],
								HRef:  versionDimension.HRef + "/codes/" + observationRowArray[i-1],
								Label: observationRowArray[i],
							}

							break
						}
					}
				}
			}
			observation.Dimensions = dimensions
		}

		observations = append(observations, observation)
	}

	// neo4j will always return the same list of observations in the same
	// order as it is deterministic for static data, but this does not
	// necessarily mean we won't want to return observations in a particular
	// order (which may be costly on the services performance)

	return observations, nil
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
	case errs.ErrTooManyWildcards:
		http.Error(w, err.Error(), http.StatusBadRequest)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
