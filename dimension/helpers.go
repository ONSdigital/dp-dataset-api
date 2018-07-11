package dimension

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/log"
	"github.com/pkg/errors"
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
	audit.LogError(ctx, errors.WithMessage(err, "request unsuccessful"), data)
	http.Error(w, resource.Error(), status)
}
