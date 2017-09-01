package api

import (

	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/ONSdigital/dp-dataset-api/api/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

const secretKey = "coffee"

var (
	internalError   = errors.New("internal error")
	badRequestError = errors.New("bad request")

	datasetPayload = "{\"contact\":{\"email\":\"testing@hotmail.com\",\"name\":\"John Cox\",\"telephone\":\"01623 456789\"},\"description\":\"census\",\"title\":\"CensusEthnicity\",\"theme\":\"population\",\"periodicity\":\"yearly\",\"state\":\"completed\",\"next_release\":\"2016-04-04\",\"publisher\":{\"name\":\"The office of national statistics\",\"type\":\"government department\",\"url\":\"https://www.ons.gov.uk/\"}}"
	editionPayload = "{\"edition\":\"2017\",\"state\":\"created\"}"
	versionPayload = "{\"edition\":\"2017\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
)

func TestGetDatasetsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetDatasetsFunc: func() (*models.DatasetResults, error) {
				return &models.DatasetResults{}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
	})
}

func TestGetDatasetsReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetDatasetsFunc: func() (*models.DatasetResults, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
	})
}

func TestGetDatasetReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetDatasetFunc: func(id string) (*models.Dataset, error) {
				return &models.Dataset{ID: "123"}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})
}

func TestGetDatasetReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetDatasetFunc: func(id string) (*models.Dataset, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionsFunc: func(id string) (*models.EditionResults, error) {
				return &models.EditionResults{}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionsReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionsFunc: func(id string) (*models.EditionResults, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionFunc: func(datasetID string, editionID string) (*models.Edition, error) {
				return &models.Edition{}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionFunc: func(datasetID string, editionID string) (*models.Edition, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetVersionsFunc: func(datasetID string, editionID string) (*models.VersionResults, error) {
				return &models.VersionResults{}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionsReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetVersionsFunc: func(datasetID string, editionID string) (*models.VersionResults, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetVersionFunc: func(datasetID string, editionID, versionID string) (*models.Version, error) {
				return &models.Version{}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetVersionFunc: func(datasetID string, editionID, versionID string) (*models.Version, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestPostDatasetsReturnsCreated(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		var b string
		b = datasetPayload
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpsertDatasetFunc: func(string, interface{}) error {
				return nil
			},
		}

		update := bson.M{
			"$set": bson.M{
				"state": "created",
			},
			"$setOnInsert": bson.M{
				"updated_at": time.Now(),
			},
		}
		mockedDataStore.UpsertDataset("123", update)

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusCreated)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 2)
	})
}

func TestPostDatasetReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		var b string
		b = "{"
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpsertDatasetFunc: func(string, interface{}) error {
				return badRequestError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		var b string
		b = datasetPayload
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpsertDatasetFunc: func(string, interface{}) error {
				return internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
	})
}

func TestPostEditionReturnsCreated(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		var b string
		b = editionPayload
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpsertEditionFunc: func(string, interface{}) error {
				return nil
			},
		}

		update := bson.M{
			"$set": bson.M{
				"state": "created",
			},
			"$setOnInsert": bson.M{
				"updated_at": time.Now(),
			},
		}
		mockedDataStore.UpsertEdition("2017", update)

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusCreated)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 2)
	})
}

func TestPostEditionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		var b string
		b = "{"
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpsertEditionFunc: func(string, interface{}) error {
				return badRequestError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		var b string
		b = editionPayload
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpsertEditionFunc: func(string, interface{}) error {
				return internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
	})
}

func TestPostVersionReturnsCreated(t *testing.T) {
	t.Parallel()
	Convey("When the json body does not contain a state", t, func() {
		var b string
		b = versionPayload
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpsertVersionFunc: func(string, interface{}) error {
				return nil
			},
		}

		update := bson.M{
			"$set": bson.M{
				"state": "created",
			},
			"$setOnInsert": bson.M{
				"updated_at": time.Now(),
			},
		}
		mockedDataStore.UpsertVersion("1", update)

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusCreated)
		So(len(mockedDataStore.UpsertVersionCalls()), ShouldEqual, 2)
	})

	Convey("When the json body contains a state of published", t, func() {
		var b string
		b = "{\"edition\":\"2017\",\"state\":\"published\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpsertVersionFunc: func(string, interface{}) error {
				return nil
			},
			UpsertDatasetFunc: func(string, interface{}) error {
				return nil
			},
		}

		update := bson.M{
			"$set": bson.M{
				"state": "published",
			},
			"$setOnInsert": bson.M{
				"updated_at": time.Now(),
			},
		}
		mockedDataStore.UpsertVersion("1", update)

		updateDataset := bson.M{
			"$set": bson.M{
				"links.latest_version.link": "http://localhost:22000/datasets/123/edition/2017/versions/1",
				"links.latest_version.id":   "1",
			},
			"$setOnInsert": bson.M{
				"updated_at": time.Now(),
			},
		}
		mockedDataStore.UpsertDataset("123", updateDataset)

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusCreated)
		So(len(mockedDataStore.UpsertVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 2)
	})
}

func TestPostVersionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		var b string
		b = "{"
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpsertVersionFunc: func(string, interface{}) error {
				return badRequestError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(len(mockedDataStore.UpsertVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		var b string
		b = versionPayload
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpsertVersionFunc: func(string, interface{}) error {
				return internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.UpsertVersionCalls()), ShouldEqual, 1)
	})

	Convey("When the api cannot connect to datastore to update dataset resource return an internal server error", t, func() {
		var b string
		b = "{\"edition\":\"2017\",\"state\":\"published\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpsertVersionFunc: func(string, interface{}) error {
				return nil
			},
			UpsertDatasetFunc: func(string, interface{}) error {
				return internalError
			},
		}

		update := bson.M{
			"$set": bson.M{
				"state": "published",
			},
			"$setOnInsert": bson.M{
				"updated_at": time.Now(),
			},
		}
		mockedDataStore.UpsertVersion("1", update)

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.UpsertVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
	})
}
