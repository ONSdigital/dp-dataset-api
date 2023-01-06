package common

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/log.go/v2/log"
	. "github.com/smartystreets/goconvey/convey"
)

type contextKey string

const requestID = contextKey("request_id")

func TestHandlePatchReqErr(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, requestID, "123789")

	t.Parallel()
	Convey("Correctly handle dimension not found", t, func() {
		w := httptest.NewRecorder()
		dimensionError := errs.ErrDimensionNotFound
		logData := log.Data{"test": "not found"}

		HandlePatchReqErr(ctx, w, dimensionError, logData)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, dimensionError.Error())
	})

	Convey("Correctly handle bad request", t, func() {
		w := httptest.NewRecorder()
		dimensionError := errs.ErrUnableToParseJSON
		logData := log.Data{"test": "bad request"}

		HandlePatchReqErr(ctx, w, dimensionError, logData)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, dimensionError.Error())
	})

	Convey("Correctly handle internal error", t, func() {
		w := httptest.NewRecorder()
		dimensionError := errs.ErrInternalServer
		logData := log.Data{"test": "internal error"}

		HandlePatchReqErr(ctx, w, dimensionError, logData)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, dimensionError.Error())
	})
}
