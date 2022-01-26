package api

import (
	"context"
	"github.com/ONSdigital/dp-dataset-api/models"
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

		dataStoreWithMockStorer := buildDataStoreWithMockCantabularBlobs([]models.Blob{})
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

		dataStoreWithMockStorer := buildDataStoreWithMockCantabularBlobs(
			[]models.Blob{
				{Name: "blob 1"},
				{Name: "blob 2"},
			},
		)
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

func buildDataStoreWithMockCantabularBlobs(fakeBlobs []models.Blob) store.DataStore {
	return store.DataStore{
		Backend: &storetest.StorerMock{
			BlobsFunc: func(ctx context.Context) (models.Blobs, error) {
				return models.Blobs{Items: fakeBlobs}, nil
			},
		},
	}
}
