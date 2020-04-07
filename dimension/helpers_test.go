package dimension

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/log.go/log"
	. "github.com/smartystreets/goconvey/convey"
)

func TestUnmarshalDimensionCache(t *testing.T) {
	t.Parallel()
	Convey("Successfully unmarshal dimension cache", t, func() {
		json := strings.NewReader(`{"option":"24", "code_list":"123-456", "dimension": "test"}`)

		option, err := unmarshalDimensionCache(json)
		So(err, ShouldBeNil)
		So(option.CodeList, ShouldEqual, "123-456")
		So(option.Name, ShouldEqual, "test")
		So(option.Option, ShouldEqual, "24")
	})

	Convey("Fail to unmarshal dimension cache", t, func() {
		Convey("When unable to marshal json", func() {
			json := strings.NewReader("{")

			option, err := unmarshalDimensionCache(json)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errs.ErrUnableToParseJSON)
			So(option, ShouldBeNil)
		})

		Convey("When options are missing mandatory fields", func() {
			json := strings.NewReader("{}")

			option, err := unmarshalDimensionCache(json)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errs.ErrMissingParameters)
			So(option, ShouldBeNil)
		})
	})
}

type contextKey string

const requestID = contextKey("request_id")

func TestHandleDimensionErr(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, requestID, "123789")

	t.Parallel()
	Convey("Correctly handle dimension not found", t, func() {
		w := httptest.NewRecorder()
		dimensionError := errs.ErrDimensionNotFound
		logData := log.Data{"test": "not found"}

		handleDimensionErr(ctx, w, dimensionError, logData)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, dimensionError.Error())
	})

	Convey("Correctly handle bad request", t, func() {
		w := httptest.NewRecorder()
		dimensionError := errs.ErrUnableToParseJSON
		logData := log.Data{"test": "bad request"}

		handleDimensionErr(ctx, w, dimensionError, logData)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldContainSubstring, dimensionError.Error())
	})

	Convey("Correctly handle internal error", t, func() {
		w := httptest.NewRecorder()
		dimensionError := errs.ErrInternalServer
		logData := log.Data{"test": "internal error"}

		handleDimensionErr(ctx, w, dimensionError, logData)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, dimensionError.Error())
	})

	Convey("Correctly handle failure to audit", t, func() {
		w := httptest.NewRecorder()
		dimensionError := errs.ErrAuditActionAttemptedFailure
		logData := log.Data{"test": "audit failure"}

		handleDimensionErr(ctx, w, dimensionError, logData)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
	})
}
