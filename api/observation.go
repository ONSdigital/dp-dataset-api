package api

import (
	"context"
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
	"github.com/ONSdigital/dp-graph/v2/observation"
	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// Upper limit, if this is not big enough, we may need to consider increasing value
// and then if this has a performance hit then consider paging
const (
	defaultObservationLimit = 10000
	defaultOffset           = 0
)

var (
	observationNotFound = map[error]bool{
		errs.ErrDatasetNotFound:      true,
		errs.ErrEditionNotFound:      true,
		errs.ErrVersionNotFound:      true,
		errs.ErrObservationsNotFound: true,
	}

	observationBadRequest = map[error]bool{
		errs.ErrTooManyWildcards: true,
	}
)

type observationQueryError struct {
	message string
}

func (e observationQueryError) Error() string {
	return e.message
}

func errorIncorrectQueryParameters(params []string) error {
	return observationQueryError{
		message: fmt.Sprintf("incorrect selection of query parameters: %v, these dimensions do not exist for this version of the dataset", params),
	}
}

func errorMissingQueryParameters(params []string) error {
	return observationQueryError{
		message: fmt.Sprintf("missing query parameters for the following dimensions: %v", params),
	}
}

func errorMultivaluedQueryParameters(params []string) error {
	return observationQueryError{
		message: fmt.Sprintf("multi-valued query parameters for the following dimensions: %v", params),
	}
}

func (api *DatasetAPI) getObservations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	datasetID := vars["dataset_id"]
	edition := vars["edition"]
	version := vars["version"]
	logData := log.Data{"dataset_id": datasetID, "edition": edition, "version": version}

	observationsDoc, err := func() (*models.ObservationsDoc, error) {
		// get dataset document
		datasetDoc, err := api.dataStore.Backend.GetDataset(datasetID)
		if err != nil {
			log.Event(ctx, "get observations: datastore.GetDataset returned an error", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		authorised := api.authenticate(r, logData)

		var (
			state   string
			dataset *models.Dataset
		)

		// if request is not authenticated then only access resources of state published
		if !authorised {
			// Check for current sub document
			if datasetDoc.Current == nil || datasetDoc.Current.State != models.PublishedState {
				logData["dataset_doc"] = datasetDoc.Current
				log.Event(ctx, "get observations: found no published dataset", log.ERROR, log.Error(errs.ErrDatasetNotFound), logData)
				return nil, errs.ErrDatasetNotFound
			}

			dataset = datasetDoc.Current
			state = dataset.State
		} else {
			dataset = datasetDoc.Next
		}

		if err = api.dataStore.Backend.CheckEditionExists(datasetID, edition, state); err != nil {
			log.Event(ctx, "get observations: failed to find edition for dataset", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		versionDoc, err := api.dataStore.Backend.GetVersion(datasetID, edition, version, state)
		if err != nil {
			log.Event(ctx, "get observations: failed to find version for dataset edition", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		if err = models.CheckState("version", versionDoc.State); err != nil {
			logData["state"] = versionDoc.State
			log.Event(ctx, "get observations: unpublished version has an invalid state", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		if versionDoc.Headers == nil || versionDoc.Dimensions == nil {
			logData["version_doc"] = versionDoc
			log.Event(ctx, "get observations", log.ERROR, log.Error(errs.ErrMissingVersionHeadersOrDimensions), logData)
			return nil, errs.ErrMissingVersionHeadersOrDimensions
		}

		// loop through version dimensions to retrieve list of dimension names
		validDimensionNames := getListOfValidDimensionNames(versionDoc.Dimensions)
		logData["version_dimensions"] = validDimensionNames

		dimensionOffset, err := getDimensionOffsetInHeaderRow(versionDoc.Headers)
		if err != nil {
			log.Event(ctx, "get observations: unable to distinguish headers from version document", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		// check query parameters match the version headers
		queryParameters, err := extractQueryParameters(r.URL.Query(), validDimensionNames)
		if err != nil {
			log.Event(ctx, "get observations: error extracting query parameters", log.ERROR, log.Error(err), logData)
			return nil, err
		}
		logData["query_parameters"] = queryParameters

		// retrieve observations
		observations, err := api.getObservationList(ctx, versionDoc, queryParameters, defaultObservationLimit, dimensionOffset, logData)
		if err != nil {
			log.Event(ctx, "get observations: unable to retrieve observations", log.ERROR, log.Error(err), logData)
			return nil, err
		}

		return models.CreateObservationsDoc(r.URL.RawQuery, versionDoc, dataset, observations, queryParameters, defaultOffset, defaultObservationLimit), nil
	}()

	if err != nil {
		handleObservationsErrorType(ctx, w, err, logData)
		return
	}

	setJSONContentType(w)

	// The ampersand "&" is escaped to "\u0026" to keep some browsers from
	// misinterpreting JSON output as HTML. This escaping can be disabled using
	// an Encoder that had SetEscapeHTML(false) called on it.
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	if err = enc.Encode(observationsDoc); err != nil {
		handleObservationsErrorType(ctx, w, errors.WithMessage(err, "failed to marshal metadata resource into bytes"), logData)
		return
	}

	log.Event(ctx, "get observations endpoint: successfully retrieved observations relative to a selected set of dimension options for a version", log.INFO, logData)
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

func getListOfValidDimensionNames(dimensions []models.Dimension) []string {

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

func (api *DatasetAPI) getObservationList(ctx context.Context, versionDoc *models.Version, queryParameters map[string]string, limit, dimensionOffset int, logData log.Data) ([]models.Observation, error) {

	// Build query (observation.Filter type)
	var dimensionFilters []*observation.Dimension

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

		dimensionFilter := &observation.Dimension{
			Name:    dimension,
			Options: []string{option},
		}

		dimensionFilters = append(dimensionFilters, dimensionFilter)
	}

	queryObject := observation.DimensionFilters{
		Dimensions: dimensionFilters,
	}

	logData["query_object"] = queryObject

	log.Event(ctx, "query object built to retrieve observations from db", log.INFO, logData)

	csvRowReader, err := api.dataStore.Backend.StreamCSVRows(ctx, versionDoc.ID, "", &queryObject, &limit)
	if err != nil {
		return nil, err
	}

	headerRow, err := csvRowReader.Read()
	if err != nil {
		return nil, err
	}

	defer csvRowReader.Close(ctx)

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

func handleObservationsErrorType(ctx context.Context, w http.ResponseWriter, err error, data log.Data) {
	_, isObservationErr := err.(observationQueryError)
	var status int
	resErrMsg := err.Error()

	switch {
	case isObservationErr:
		status = http.StatusBadRequest
	case observationNotFound[err]:
		status = http.StatusNotFound
	case observationBadRequest[err]:
		status = http.StatusBadRequest
	default:
		resErrMsg = errs.ErrInternalServer.Error()
		status = http.StatusInternalServerError
	}

	if data == nil {
		data = log.Data{}
	}

	data["responseStatus"] = status
	log.Event(ctx, "get observation endpoint: request unsuccessful", log.ERROR, log.Error(err), data)
	http.Error(w, resErrMsg, status)
}
