package utils

import (
	"net/url"
	"strconv"
	"strings"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
)

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
