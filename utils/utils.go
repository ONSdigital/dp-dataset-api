package utils

import (
	"net/url"
	"strconv"
	"strings"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/globalsign/mgo/bson"
)

// GetPositiveIntQueryParameter obtains the positive int value of query var defined by the provided varKey
func GetPositiveIntQueryParameter(queryVars url.Values, varKey string, defaultValue int) (val int, err error) {
	strVal, found := queryVars[varKey]
	if !found {
		return defaultValue, nil
	}
	val, err = strconv.Atoi(strVal[0])
	if err != nil {
		return -1, errs.ErrInvalidQueryParameter
	}
	if val < 0 {
		return 0, nil
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

// utility function to cut a slice according to the provided offset and limit.
// limit=0 means no limit, and values higher than the slice length are ignored
func Slice(full []bson.M, offset, limit int) (sliced []bson.M) {
	end := offset + limit
	if limit == 0 || end > len(full) {
		end = len(full)
	}

	if offset > len(full) {
		return []bson.M{}
	}
	return full[offset:end]
}
