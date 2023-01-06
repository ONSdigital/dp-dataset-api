package common

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	dprequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"
)

func HandlePatchReqErr(ctx context.Context, w http.ResponseWriter, err error, data log.Data) {
	if data == nil {
		data = log.Data{}
	}

	var status int

	// Switch by error type
	switch err.(type) {
	case errs.ErrInvalidPatch:
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

func HandleDatasetAPIErr(ctx context.Context, err error, w http.ResponseWriter, data log.Data) {
	if data == nil {
		data = log.Data{}
	}

	var status int
	switch {
	case datasetsForbidden[err]:
		status = http.StatusForbidden
	case datasetsNoContent[err]:
		status = http.StatusNoContent
	case datasetsBadRequest[err], strings.HasPrefix(err.Error(), "invalid fields:"):
		status = http.StatusBadRequest
	case resourcesNotFound[err]:
		status = http.StatusNotFound
	default:
		err = fmt.Errorf("%s: %w", errs.ErrInternalServer.Error(), err)
		status = http.StatusInternalServerError
	}

	data["responseStatus"] = status
	log.Error(ctx, "request unsuccessful", err, data)
	http.Error(w, err.Error(), status)
}
