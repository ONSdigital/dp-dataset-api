package dimension

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/apierrors"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	dprequest "github.com/ONSdigital/dp-net/request"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"
)

const (
	reqUser   = "req_user"
	reqCaller = "req_caller"
)

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
			err = errors.WithMessage(err, errs.ErrInternalServer.Error())
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
