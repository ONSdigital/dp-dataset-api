package api_test

import (
	"context"
	"errors"
	"github.com/ONSdigital/dp-dataset-api/api"
	"net/http"
	"testing"

	"net/http/httptest"

	"github.com/ONSdigital/dp-dataset-api/api/mock"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAPIRouteRegistration(t *testing.T) {

	Convey("Given the data set API is created", t, func() {
		api := buildAPI(cantabularClientReturningData(nil, nil), nil)

		Convey("When I GET /population-types", func() {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/population-types", nil)
			api.Router.ServeHTTP(rec, req)

			SoMsg("Then it should return with 200",
				rec.Code, ShouldEqual, 200)
		})
	})
}

func TestPopulationTypesRootHappyPath(t *testing.T) {

	Convey("Given the data set API is created", t, func() {
		cantabularClient := cantabularClientReturningData([]string{"dataset 1", "dataset 2"}, nil)
		api := buildAPI(cantabularClient, nil)

		Convey("When I GET /population-types", func() {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/population-types", nil)
			api.Router.ServeHTTP(rec, req)

			SoMsg("Then it should return application/json content",
				rec.Header().Get("Content-Type"), ShouldEqual, "application/json")
			SoMsg("Then it should return expected JSON",
				rec.Body.String(), ShouldEqual, `{"items":[{"name":"dataset 1"},{"name":"dataset 2"}]}`+"\n")
		})
	})
}

func TestPopulationTypesRootUnhappyPath(t *testing.T) {

	Convey("Given the data set API is created but the cantabular client fails", t, func() {

		loggerMock := mock.LoggerMock{
			ErrorFunc: func(ctx context.Context, event string, err error) {},
		}

		errorText := "oh no no no no no"
		cantabularClient := cantabularClientReturningData(nil, errors.New(errorText))
		api := buildAPI(cantabularClient, &loggerMock)

		Convey("When I GET /population-types", func() {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/population-types", nil)
			api.Router.ServeHTTP(rec, req)

			SoMsg("Then it should return the expected error",
				rec.Code, ShouldEqual, http.StatusInternalServerError)
			SoMsg("Then it should indicate that fetching population types failed",
				rec.Body.String(), ShouldEqual, "failed to fetch population types\n")
			Convey("Then it should log the correct information", func() {
				actualErrors := loggerMock.ErrorCalls()
				So(actualErrors, ShouldHaveLength, 1)
				So(actualErrors[0].Event, ShouldEqual, "error retrieving datasets from cantabular")
				So(actualErrors[0].Err, ShouldResemble, errors.New(errorText))
			})
		})
	})

	Convey("Given the data set API is created", t, func() {

		loggerMock := mock.LoggerMock{
			ErrorFunc: func(ctx context.Context, event string, err error) {},
		}
		api := buildAPI(cantabularClientReturningData(nil, nil), &loggerMock)

		Convey("When I GET /population-types but writing fails", func() {

			req := httptest.NewRequest("GET", "/population-types", nil)
			responseWriter := FailingWriter{}
			api.GetPopulationTypesHandler(&responseWriter, req)
			SoMsg("Then it should return a 500 status code",
				responseWriter.statusCode, ShouldEqual, http.StatusInternalServerError)
			SoMsg("Then it should respond with error message",
				responseWriter.attemptedWrite[0], ShouldEqual, "failed to respond with population types\n")
			Convey("Then it should log the correct information", func() {
				actualErrors := loggerMock.ErrorCalls()
				So(actualErrors, ShouldHaveLength, 1)
				So(actualErrors[0].Event, ShouldEqual, "failed to encode and write population types model to response object")
				So(actualErrors[0].Err, ShouldResemble, errors.New(failingWriterErrorText))
			})

		})
	})
}

const failingWriterErrorText = "oops"

type FailingWriter struct {
	statusCode     int
	attemptedWrite []string
}

func (f *FailingWriter) Header() http.Header {
	return http.Header{}
}

func (f *FailingWriter) Write(data []byte) (int, error) {
	f.attemptedWrite = append([]string{string(data)}, f.attemptedWrite...)
	return 0, errors.New(failingWriterErrorText)
}

func (f *FailingWriter) WriteHeader(statusCode int) {
	f.statusCode = statusCode
}

func cantabularClientReturningData(strings []string, err error) api.CantabularClient {
	return &mock.CantabularClientMock{
		ListDatasetsFunc: func(_ context.Context) ([]string, error) {
			return strings, err
		},
	}
}

func buildAPI(cantabularClient api.CantabularClient, loggerMock *mock.LoggerMock) *api.DatasetAPI {
	cfg, err := config.Get()
	if err != nil {
		panic(err)
	}
	fakeAuthHandler := &mocks.AuthHandlerMock{Required: &mocks.PermissionCheckCalls{}}
	return api.Setup(context.Background(), cfg, mux.NewRouter(), store.DataStore{}, nil, nil, fakeAuthHandler, fakeAuthHandler, cantabularClient, loggerMock)
}
