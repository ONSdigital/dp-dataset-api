package dimension

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"regexp"

	"github.com/ONSdigital/dp-dataset-api/apierrors"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	dprequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"
)

const (
	reqUser   = "req_user"
	reqCaller = "req_caller"
)

// Regex const for patch paths
var (
	optionNodeIDRegex = regexp.MustCompile("/([^/]+)/options/([^/]+)/node_id")
	optionOrderRegex  = regexp.MustCompile("/([^/]+)/options/([^/]+)/order")
	slashRegex        = regexp.MustCompile("/")
)

// isNodeIDPath checks if the provided string matches '/{dimension}/options/{option}/node_id' exactly once
func isNodeIDPath(p string) bool {
	return len(optionNodeIDRegex.FindAllString(p, -1)) == 1
}

// isOrderPath checks if the provided string matches '/{dimension}/options/{option}/order' exactly once
func isOrderPath(p string) bool {
	return len(optionOrderRegex.FindAllString(p, -1)) == 1
}

// createOptionFromPath creates a *DimensionOption struct pointer from the provided path, containing only Name and Option
// note that this method assumes that the path has already been validated (with isNodeIDPath or isOrderPath)
func createOptionFromPath(p string) *models.DimensionOption {
	spl := slashRegex.Split(p, 5)
	return &models.DimensionOption{
		Name:   spl[1], // {dimension} value from patch path
		Option: spl[3], // {option} value from patch path
	}
}

// getOptionsArrayFromInterface obtains an array of *CachedDimensionOption from the provided interface
func getOptionsArrayFromInterface(elements interface{}) ([]*models.CachedDimensionOption, error) {
	options := []*models.CachedDimensionOption{}

	// elements should be an array
	arr, ok := elements.([]interface{})
	if !ok {
		return options, errors.New("missing list of items")
	}

	// each item in the array should be an option
	for _, v := range arr {
		// need to re-marshal, as it is currently a map
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}

		// unmarshal and validate CachedDimensionOption structure
		option, err := unmarshalDimensionCache(bytes.NewBuffer(b))
		if err != nil {
			return nil, err
		}
		options = append(options, option)
	}

	return options, nil
}

func unmarshalDimensionCache(reader io.Reader) (*models.CachedDimensionOption, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errs.ErrUnableToReadMessage
	}

	var option models.CachedDimensionOption

	err = json.Unmarshal(b, &option)
	if err != nil {
		return nil, errs.ErrUnableToParseJSON
	}
	if option.Name == "" || (option.Option == "" && option.CodeList == "") {
		return nil, errs.ErrMissingParameters
	}

	return &option, nil
}

func handleDimensionErr(ctx context.Context, w http.ResponseWriter, err error, data log.Data) {
	if data == nil {
		data = log.Data{}
	}

	var status int

	// Switch by error type
	switch err.(type) {
	case apierrors.ErrInvalidPatch:
		status = http.StatusBadRequest
	default:
		// Switch by error message
		switch {
		case errs.NotFoundMap[err]:
			status = http.StatusNotFound
		case errs.BadRequestMap[err]:
			status = http.StatusBadRequest
		case errs.ConflictRequestMap[err]:
			status = http.StatusConflict
		default:
			status = http.StatusInternalServerError
			err = errors.WithMessage(err, "internal error")
		}
	}

	data["response_status"] = status
	logError(ctx, err, data)
	http.Error(w, err.Error(), status)
}

func logError(ctx context.Context, err error, data log.Data) {
	if user := dprequest.User(ctx); user != "" {
		data[reqUser] = user
	}
	if caller := dprequest.Caller(ctx); caller != "" {
		data[reqCaller] = caller
	}
	log.Error(ctx, "unsuccessful request", err, data)
}

func getIfMatch(r *http.Request) string {
	ifMatch := r.Header.Get("If-Match")
	if ifMatch == "" {
		return mongo.AnyETag
	}
	return ifMatch
}

func setJSONPatchContentType(w http.ResponseWriter) {
	w.Header().Add("Content-Type", "application/json-patch+json")
}

func writeBody(ctx context.Context, w http.ResponseWriter, b []byte, data log.Data) {
	if _, err := w.Write(b); err != nil {
		log.Error(ctx, "failed to write response body", err, data)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
