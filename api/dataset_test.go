package api

import (
	"github.com/ONSdigital/dp-dataset-api/api/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
	"net/http"
	"net/http/httptest"
	"testing"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
)

func TestGetDatasetsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/datasets", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &datastoretest.DataStoreMock{
			GetAllDatasetsFunc: func() (*models.DatasetResults, error) {
				return &models.DatasetResults{}, nil
			},
		}
		mockedDataStore.GetAllDatasetsCalls()

		api := CreateDatasetAPI("123", mux.NewRouter(), mockedDataStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetAllDatasetsCalls()), ShouldEqual, 1)
	})
}

func TestGetDatasetsReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/datasets", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &datastoretest.DataStoreMock{
			GetAllDatasetsFunc: func() (*models.DatasetResults, error) {
				return nil, errors.New("internal error")
			},
		}
		mockedDataStore.GetAllDatasetsCalls()

		api := CreateDatasetAPI("123", mux.NewRouter(), mockedDataStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetAllDatasetsCalls()), ShouldEqual, 1)
	})
}
