package utils

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
)

// DistributionMediaTypeMap maps distribution formats to their corresponding media types
var DistributionMediaTypeMap = map[models.DistributionFormat]models.DistributionMediaType{
	models.DistributionFormatCSV:      models.DistributionMediaTypeCSV,
	models.DistributionFormatSDMX:     models.DistributionMediaTypeSDMX,
	models.DistributionFormatXLS:      models.DistributionMediaTypeXLS,
	models.DistributionFormatXLSX:     models.DistributionMediaTypeXLSX,
	models.DistributionFormatCSDB:     models.DistributionMediaTypeCSDB,
	models.DistributionFormatCSVWMeta: models.DistributionMediaTypeCSVWMeta,
}

// ValidatePositiveInt obtains the positive int value of query var defined by the provided varKey
func ValidatePositiveInt(parameter string) (val int, err error) {
	val, err = strconv.Atoi(parameter)
	if err != nil {
		return -1, errs.ErrInvalidQueryParameter
	}
	if val < 0 {
		return -1, errs.ErrInvalidQueryParameter
	}
	return val, nil
}

// GetQueryParamListValues obtains a list of strings from the provided queryVars,
// by parsing all values with key 'varKey' and splitting the values by commas, if they contain commas.
// Up to maxNumItems values are allowed in total.
func GetQueryParamListValues(queryVars url.Values, varKey string, maxNumItems int) (items []string, err error) {
	// get query parameters values for the provided key
	values, found := queryVars[varKey]
	if !found {
		return []string{}, nil
	}

	// each value may contain a simple value or a list of values, in a comma-separated format
	for _, value := range values {
		items = append(items, strings.Split(value, ",")...)
		if len(items) > maxNumItems {
			return []string{}, errs.ErrTooManyQueryParameters
		}
	}
	return items, nil
}

// Slice is a utility function to cut a slice according to the provided offset and limit.
func Slice(full []models.Dimension, offset, limit int) (sliced []models.Dimension) {
	end := offset + limit
	if end > len(full) {
		end = len(full)
	}

	if offset > len(full) {
		return []models.Dimension{}
	}
	return full[offset:end]
}

// SliceStr is a utility function to cut a slice of *strings according to the provided offset and limit.
func SliceStr(full []*string, offset, limit int) (sliced []*string) {
	end := offset + limit
	if end > len(full) {
		end = len(full)
	}

	if offset > len(full) {
		return []*string{}
	}
	return full[offset:end]
}

func BuildTopics(canonicalTopic string, subtopics []string) []string {
	topics := []string{}
	if canonicalTopic != "" {
		topics = append(topics, canonicalTopic)
	}
	if subtopics != nil {
		topics = append(topics, subtopics...)
	}
	return topics
}

// PopulateDistributions populates the MediaType field for each distribution based on its Format field
func PopulateDistributions(v *models.Version) error {
	if v.Distributions == nil {
		return nil
	}

	for i, dist := range *v.Distributions {
		if dist.Format == "" {
			return fmt.Errorf("distributions[%d].format field is missing", i)
		}
		mediaType, ok := DistributionMediaTypeMap[dist.Format]
		if !ok {
			return fmt.Errorf("distributions[%d].format field is invalid", i)
		}
		(*v.Distributions)[i].MediaType = mediaType
	}

	return nil
}

// ValidateDistributionsFromRequestBody validates distributions in the raw JSON request body
// to provide detailed error messages with the index of invalid formats
func ValidateDistributionsFromRequestBody(bodyBytes []byte) error {
	// Parse just the distributions array from the raw JSON
	var rawData map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &rawData); err != nil {
		return nil // if we can't parse at all, let the main unmarshal handle it
	}

	distributions, ok := rawData["distributions"]
	if !ok {
		return nil // no distributions field, let the main unmarshal handle it
	}

	distArray, ok := distributions.([]interface{})
	if !ok {
		return nil // not an array, let the main unmarshal handle it
	}

	for i, dist := range distArray {
		distMap, ok := dist.(map[string]interface{})
		if !ok {
			return fmt.Errorf("distributions[%d] is not a valid object", i)
		}

		formatVal, ok := distMap["format"]
		if !ok {
			return fmt.Errorf("distributions[%d].format field is missing", i)
		}

		formatStr, ok := formatVal.(string)
		if !ok {
			return fmt.Errorf("distributions[%d].format field is invalid", i)
		}

		if _, valid := DistributionMediaTypeMap[models.DistributionFormat(formatStr)]; !valid {
			return fmt.Errorf("distributions[%d].format field is invalid", i)
		}
	}

	return nil
}
