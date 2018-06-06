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
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
)

//go:generate moq -out ../mocks/observation_store.go -pkg mocks . ObservationStore

// Upper limit, if this is not big enough, we may need to consider increasing value
// and then if this has a performance hit then consider paging
const (
	defaultObservationLimit = 10000
	defaultOffset           = 0

	getObservationsAction = "getObservations"
)

func errorIncorrectQueryParameters(params []string) error {
	return fmt.Errorf("Incorrect selection of query parameters: %v, these dimensions do not exist for this version of the dataset", params)
}

func errorMissingQueryParameters(params []string) error {
	return fmt.Errorf("Missing query parameters for the following dimensions: %v", params)
}

func errorMultivaluedQueryParameters(params []string) error {
	return fmt.Errorf("Multi-valued query parameters for the following dimensions: %v", params)
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
	auditParams := common.Params{"dataset_id": datasetID, "edition": edition, "version": version}

	if auditErr := api.auditor.Record(r.Context(), getObservationsAction, audit.Attempted, auditParams); auditErr != nil {
		handleAuditingFailure(w, auditErr, logData)
		return
	}

	// get dataset document
	datasetDoc, err := api.dataStore.Backend.GetDataset(datasetID)
	if err != nil {
		log.Error(err, logData)
		if auditErr := api.auditor.Record(r.Context(), getObservationsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleObservationsErrorType(w, err)
		return
	}

	authorised, logData := api.authenticate(r, logData)

	var (
		state   string
		dataset *models.Dataset
	)

	// if request is not authenticated then only access resources of state published
	if !authorised {
		// Check for current sub document
		if datasetDoc.Current == nil || datasetDoc.Current.State != models.PublishedState {
			logData["dataset_doc"] = datasetDoc.Current
			log.ErrorC("found no published dataset", errs.ErrDatasetNotFound, logData)
			if auditErr := api.auditor.Record(r.Context(), getObservationsAction, audit.Unsuccessful, auditParams); auditErr != nil {
				handleAuditingFailure(w, auditErr, logData)
				return
			}
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
		if auditErr := api.auditor.Record(r.Context(), getObservationsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleObservationsErrorType(w, err)
		return
	}

	versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, state)
	if err != nil {
		log.ErrorC("failed to find version for dataset edition", err, logData)
		if auditErr := api.auditor.Record(r.Context(), getObservationsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleObservationsErrorType(w, err)
		return
	}

	if err = models.CheckState("version", versionDoc.State); err != nil {
		logData["state"] = versionDoc.State
		log.ErrorC("unpublished version has an invalid state", err, logData)
		if auditErr := api.auditor.Record(r.Context(), getObservationsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleObservationsErrorType(w, err)
		return
	}

	if versionDoc.Headers == nil || versionDoc.Dimensions == nil {
		logData["version_doc"] = versionDoc
		log.Error(errs.ErrMissingVersionHeadersOrDimensions, logData)
		if auditErr := api.auditor.Record(r.Context(), getObservationsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		http.Error(w, "", http.StatusInternalServerError)
		return
	}

	// loop through version dimensions to retrieve list of dimension names
	validDimensionNames := getListOfValidDimensionNames(versionDoc.Dimensions)
	logData["version_dimensions"] = validDimensionNames

	dimensionOffset, err := getDimensionOffsetInHeaderRow(versionDoc.Headers)
	if err != nil {
		log.ErrorC("unable to distinguish headers from version document", err, logData)
		if auditErr := api.auditor.Record(r.Context(), getObservationsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleObservationsErrorType(w, err)
		return
	}

	// check query parameters match the version headers
	queryParameters, err := extractQueryParameters(r.URL.Query(), validDimensionNames)
	if err != nil {
		log.Error(err, logData)
		if auditErr := api.auditor.Record(r.Context(), getObservationsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	logData["query_parameters"] = queryParameters

	// retrieve observations
	observations, err := api.getObservationList(versionDoc, queryParameters, defaultObservationLimit, dimensionOffset, logData)
	if err != nil {
		log.ErrorC("unable to retrieve observations", err, logData)
		if auditErr := api.auditor.Record(r.Context(), getObservationsAction, audit.Unsuccessful, auditParams); auditErr != nil {
			handleAuditingFailure(w, auditErr, logData)
			return
		}
		handleObservationsErrorType(w, err)
		return
	}

	observationsDoc := models.CreateObservationsDoc(r.URL.RawQuery, versionDoc, dataset, observations, queryParameters, defaultOffset, defaultObservationLimit)

	if auditErr := api.auditor.Record(r.Context(), getObservationsAction, audit.Successful, auditParams); auditErr != nil {
		handleAuditingFailure(w, auditErr, logData)
		return
	}

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

func getDimensionOffsetInHeaderRow(headerRow []string) (int, error) {
	metaData := strings.Split(headerRow[0], "_")

	if len(metaData) < 2 {
		return 0, errs.ErrIndexOutOfRange
	}

	dimensionOffset, err := strconv.Atoi(metaData[1])
	if err != nil {
		return 0, err
	}

	return dimensionOffset, nil
}

func getListOfValidDimensionNames(dimensions []models.CodeList) []string {

	var dimensionNames []string
	for _, dimension := range dimensions {
		dimensionNames = append(dimensionNames, dimension.Name)
	}

	return dimensionNames
}

func extractQueryParameters(urlQuery url.Values, validDimensions []string) (map[string]string, error) {
	queryParameters := make(map[string]string)
	var incorrectQueryParameters, missingQueryParameters, multivaluedQueryParameters []string

	// Determine if any request query parameters are invalid dimensions
	// and map the valid dimensions with their equivalent values in map
	for rawDimension, option := range urlQuery {
		// Ignore case sensitivity
		dimension := strings.ToLower(rawDimension)

		queryParamExists := false
		for _, validDimension := range validDimensions {
			if dimension == validDimension {
				queryParamExists = true
				queryParameters[dimension] = option[0]
				if len(option) != 1 {
					multivaluedQueryParameters = append(multivaluedQueryParameters, rawDimension)
				}
				break
			}
		}
		if !queryParamExists {
			incorrectQueryParameters = append(incorrectQueryParameters, rawDimension)
		}
	}

	if len(incorrectQueryParameters) > 0 {
		return nil, errorIncorrectQueryParameters(incorrectQueryParameters)
	}

	if len(multivaluedQueryParameters) > 0 {
		return nil, errorMultivaluedQueryParameters(multivaluedQueryParameters)
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

func (api *DatasetAPI) getObservationList(versionDoc *models.Version, queryParameters map[string]string, limit, dimensionOffset int, logData log.Data) ([]models.Observation, error) {

	// Build query (observation.Filter type)
	var dimensionFilters []*observation.DimensionFilter

	// Unable to have more than one wildcard parameter per query
	var wildcardParameter string

	// Build dimension filter object to create queryObject for neo4j
	for dimension, option := range queryParameters {
		if option == "*" {
			if wildcardParameter != "" {
				return nil, errs.ErrTooManyWildcards
			}

			wildcardParameter = dimension
			continue
		}

		dimensionFilter := &observation.DimensionFilter{
			Name:    dimension,
			Options: []string{option},
		}

		dimensionFilters = append(dimensionFilters, dimensionFilter)
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
	for observationRow, err = csvRowReader.Read(); err != io.EOF; observationRow, err = csvRowReader.Read() {
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

			for i := dimensionOffset + 2; i < len(observationRowArray); i += 2 {

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

					break
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
