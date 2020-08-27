package dimension

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	dprequest "github.com/ONSdigital/dp-net/request"
	"github.com/ONSdigital/log.go/log"
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
	resource := err
	switch {
	case errs.NotFoundMap[err]:
		status = http.StatusNotFound
	case errs.BadRequestMap[err]:
		status = http.StatusBadRequest
	default:
		status = http.StatusInternalServerError
		resource = errs.ErrInternalServer
	}

	data["response_status"] = status
	logError(ctx, errors.WithMessage(err, "request unsuccessful"), data)
	http.Error(w, resource.Error(), status)
}

func logError(ctx context.Context, err error, data log.Data) error {
	if user := dprequest.User(ctx); user != "" {
		data[reqUser] = user
	}
	if caller := dprequest.Caller(ctx); caller != "" {
		data[reqCaller] = caller
	}
	err = errors.WithMessage(err, "putVersion endpoint: failed to set instance node is_published")
	log.Event(ctx, "failed to publish instance version", log.ERROR, log.Error(err), data)
	return err
}
