package api

import (
	"context"
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

		api := Setup(testContext, cfg, mux.NewRouter(), store.DataStore{}, nil, nil, nil, nil)

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
		fakeBlobs := []store.CantabularBlob{
			{Name: "blob 1"},
			{Name: "blob 2"},
		}
		dataStoreWithMockStorer := funcName(fakeBlobs)
		api := Setup(testContext, cfg, mux.NewRouter(), dataStoreWithMockStorer, nil, nil, nil, nil)

		Convey("When I GET /census", func() {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/census", nil)
			api.Router.ServeHTTP(rec, req)

			SoMsg("Then it should return application/json content",
				rec.Header().Get("Content-Type"), ShouldEqual, "application/json")
			SoMsg("Then it should return expected JSON",
				rec.Body.String(), ShouldEqual, `{"items"":[{"name":"blob 1"},{"name":"blob 2"}]}`)
		})
	})
}

func funcName(fakeBlobs []store.CantabularBlob) store.DataStore {
	dataStoreWithMockStorer := store.DataStore{
		Backend: &storetest.StorerMock{
			BlobsFunc: func(ctx context.Context) ([]store.CantabularBlob, error) {
				return fakeBlobs, nil
			},
		},
	}
	return dataStoreWithMockStorer
}
