package api

import (
	"context"
	"errors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"net/http"
	"testing"

	"net/http/httptest"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAPIRouteRegistration(t *testing.T) {

	Convey("Given the data set API is created", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)

		dataStoreWithMockStorer := buildDataStoreWithFakePopulationTypes([]models.PopulationType{}, nil)
		api := Setup(testContext, cfg, mux.NewRouter(), dataStoreWithMockStorer, nil, nil, nil, nil)

		Convey("When I GET /census", func() {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/census", nil)
			api.Router.ServeHTTP(rec, req)

			SoMsg("Then it should return with 200",
				rec.Code, ShouldEqual, 200)
		})
	})
}

func TestCensusRootHappyPath(t *testing.T) {

	Convey("Given the data set API is created", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)

		dataStoreWithMockStorer := buildDataStoreWithFakePopulationTypes(
			[]models.PopulationType{
				{Name: "blob 1"},
				{Name: "blob 2"},
			},
			nil)
		api := Setup(testContext, cfg, mux.NewRouter(), dataStoreWithMockStorer, nil, nil, nil, nil)

		Convey("When I GET /census", func() {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/census", nil)
			api.Router.ServeHTTP(rec, req)

			SoMsg("Then it should return application/json content",
				rec.Header().Get("Content-Type"), ShouldEqual, "application/json")
			SoMsg("Then it should return expected JSON",
				rec.Body.String(), ShouldEqual, `{"items":[{"name":"blob 1"},{"name":"blob 2"}]}`)
		})
	})
}

func TestCensusRootUnhappyPath(t *testing.T) {
	Convey("Given the data set API is created but the data store fails", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)

		dataStoreWithMockStorer := buildDataStoreWithFakePopulationTypes(nil, errors.New("oh no no no no no"))
		api := Setup(testContext, cfg, mux.NewRouter(), dataStoreWithMockStorer, nil, nil, nil, nil)

		Convey("When I GET /census", func() {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/census", nil)
			api.Router.ServeHTTP(rec, req)

			SoMsg("Then it should return the expected error",
				rec.Code, ShouldEqual, http.StatusInternalServerError)
			SoMsg("Then it should indicate that fetching population types failed",
				rec.Body.String(), ShouldEqual, "failed to fetch population types\n")
		})
	})

	Convey("Given the data set API is created", t, func() {

		api := DatasetAPI{
			dataStore: buildDataStoreWithFakePopulationTypes(nil, nil),
		}
		Convey("When I GET /census but writing fails", func() {
			req := httptest.NewRequest("GET", "/census", nil)
			responseWriter := FailingWriter{}
			api.getCensus(&responseWriter, req)
			SoMsg("Then it should return a 500 status code",
				responseWriter.statusCode, ShouldEqual, http.StatusInternalServerError)
			SoMsg("Should respond with error message",
				responseWriter.attemptedWrite[0], ShouldEqual, "failed to respond with population types\n")
		})
	})
}

type FailingWriter struct {
	statusCode     int
	attemptedWrite []string
}

func (f *FailingWriter) Header() http.Header {
	return http.Header{}
}

func (f *FailingWriter) Write(data []byte) (int, error) {
	f.attemptedWrite = append([]string{string(data)}, f.attemptedWrite...)
	return 0, errors.New("oops")
}

func (f *FailingWriter) WriteHeader(statusCode int) {
	f.statusCode = statusCode
}

func buildDataStoreWithFakePopulationTypes(populationTypes []models.PopulationType, errorToReturn error) store.DataStore {
	return store.DataStore{
		Backend: &storetest.StorerMock{
			PopulationTypesFunc: func(ctx context.Context) ([]models.PopulationType, error) {
				return populationTypes, errorToReturn
			},
		},
	}
}
