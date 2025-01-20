package dimension

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/smartystreets/goconvey/convey"
)

func TestUnmarshalDimensionCache(t *testing.T) {
	t.Parallel()
	convey.Convey("Successfully unmarshal dimension cache", t, func() {
		json := strings.NewReader(`{"option":"24", "code_list":"123-456", "dimension": "test"}`)

		option, err := unmarshalDimensionCache(json)
		convey.So(err, convey.ShouldBeNil)
		convey.So(option.CodeList, convey.ShouldEqual, "123-456")
		convey.So(option.Name, convey.ShouldEqual, "test")
		convey.So(option.Option, convey.ShouldEqual, "24")
	})

	convey.Convey("Fail to unmarshal dimension cache", t, func() {
		convey.Convey("When unable to marshal json", func() {
			json := strings.NewReader("{")

			option, err := unmarshalDimensionCache(json)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldResemble, errs.ErrUnableToParseJSON)
			convey.So(option, convey.ShouldBeNil)
		})

		convey.Convey("When options are missing mandatory fields", func() {
			json := strings.NewReader("{}")

			option, err := unmarshalDimensionCache(json)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldResemble, errs.ErrMissingParameters)
			convey.So(option, convey.ShouldBeNil)
		})
	})
}

type contextKey string

const requestID = contextKey("request_id")

func TestHandleDimensionErr(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, requestID, "123789")

	t.Parallel()
	convey.Convey("Correctly handle dimension not found", t, func() {
		w := httptest.NewRecorder()
		dimensionError := errs.ErrDimensionNotFound
		logData := log.Data{"test": "not found"}

		handleDimensionErr(ctx, w, dimensionError, logData)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, dimensionError.Error())
	})

	convey.Convey("Correctly handle bad request", t, func() {
		w := httptest.NewRecorder()
		dimensionError := errs.ErrUnableToParseJSON
		logData := log.Data{"test": "bad request"}

		handleDimensionErr(ctx, w, dimensionError, logData)

		convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, dimensionError.Error())
	})

	convey.Convey("Correctly handle internal error", t, func() {
		w := httptest.NewRecorder()
		dimensionError := errs.ErrInternalServer
		logData := log.Data{"test": "internal error"}

		handleDimensionErr(ctx, w, dimensionError, logData)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, dimensionError.Error())
	})
}
