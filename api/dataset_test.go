package api

import (
	"github.com/ONSdigital/dp-dataset-api/api/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/gorilla/mux"
	"github.com/johnnadratowski/golang-neo4j-bolt-driver/errors"
	. "github.com/smartystreets/goconvey/convey"
	"net/http"
	"net/http/httptest"
	"testing"
)

var internalError = errors.New("internal error")

func TestGetDatasetsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/datasets", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &datastoretest.DataStoreMock{
			GetDatasetsFunc: func() (*models.DatasetResults, error) {
				return &models.DatasetResults{}, nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), mockedDataStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
	})
}

func TestGetDatasetsReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/datasets", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &datastoretest.DataStoreMock{
			GetDatasetsFunc: func() (*models.DatasetResults, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), mockedDataStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
	})
}

func TestGetDatasetReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/datasets/123-456", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &datastoretest.DataStoreMock{
			GetDatasetFunc: func(id string) (*models.Dataset, error) {
				return &models.Dataset{ID: "123"}, nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), mockedDataStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})
}

func TestGetDatasetReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/datasets/123-456", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &datastoretest.DataStoreMock{
			GetDatasetFunc: func(id string) (*models.Dataset, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), mockedDataStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/datasets/123-456/editions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &datastoretest.DataStoreMock{
			GetEditionsFunc: func(id string) (*models.EditionResults, error) {
				return &models.EditionResults{}, nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), mockedDataStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionsReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/datasets/123-456/editions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &datastoretest.DataStoreMock{
			GetEditionsFunc: func(id string) (*models.EditionResults, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), mockedDataStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/datasets/123-456/editions/678", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &datastoretest.DataStoreMock{
			GetEditionFunc: func(datasetID string, editionID string) (*models.Edition, error) {
				return &models.Edition{}, nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), mockedDataStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/datasets/123-456/editions/678", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &datastoretest.DataStoreMock{
			GetEditionFunc: func(datasetID string, editionID string) (*models.Edition, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), mockedDataStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})
}


func TestGetVersionsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/datasets/123-456/editions/678/versions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &datastoretest.DataStoreMock{
			GetVersionsFunc: func(datasetID string, editionID string) (*models.VersionResults, error) {
				return &models.VersionResults{}, nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), mockedDataStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionsReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/datasets/123-456/editions/678/versions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &datastoretest.DataStoreMock{
			GetVersionsFunc: func(datasetID string, editionID string) (*models.VersionResults, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), mockedDataStore)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})
}