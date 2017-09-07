package api

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/ONSdigital/dp-dataset-api/api-errors"
	"github.com/ONSdigital/dp-dataset-api/api/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

const secretKey = "coffee"

var (
	internalError   = errors.New("internal error")
	badRequestError = errors.New("bad request")
	notFoundError   = errors.New("not found")

	datasetPayload           = "{\"contact\":{\"email\":\"testing@hotmail.com\",\"name\":\"John Cox\",\"telephone\":\"01623 456789\"},\"description\":\"census\",\"title\":\"CensusEthnicity\",\"theme\":\"population\",\"periodicity\":\"yearly\",\"state\":\"completed\",\"next_release\":\"2016-04-04\",\"publisher\":{\"name\":\"The office of national statistics\",\"type\":\"government department\",\"url\":\"https://www.ons.gov.uk/\"}}"
	editionPayload           = "{\"edition\":\"2017\",\"state\":\"created\"}"
	versionPayload           = "{\"edition\":\"2017\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
	versionAssociatedPayload = "{\"edition\":\"2017\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\",\"state\":\"associated\",\"collection_id\":\"12345\"}"
	versionPublishedPayload  = "{\"edition\":\"2017\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\",\"state\":\"published\",\"collection_id\":\"12345\"}"
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

func TestGetDatasetsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
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
	Convey("When dataset document has a current sub document return status 200", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Current: &models.Dataset{ID: "123"}}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When dataset document has only a next sub document and request contains valid internal_token return status 200", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		So(err, ShouldBeNil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{ID: "123"}}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})
}

func TestGetDatasetReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When dataset document has only a next sub document return status 404", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{ID: "123"}}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When there is no dataset document return status 404", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return nil, api_errors.DatasetNotFound
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
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
			GetEditionsFunc: func(id string, selector interface{}) (*models.EditionResults, error) {
				return &models.EditionResults{}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionsFunc: func(id string, selector interface{}) (*models.EditionResults, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})

	Convey("When no editions exist against an existing dataset return status not found", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		So(err, ShouldBeNil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionsFunc: func(id string, selector interface{}) (*models.EditionResults, error) {
				return nil, api_errors.EditionNotFound
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})

	Convey("When no published editions exist against a published dataset return status not found", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionsFunc: func(id string, selector interface{}) (*models.EditionResults, error) {
				return nil, api_errors.EditionNotFound
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
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
			GetEditionFunc: func(selector interface{}) (*models.Edition, error) {
				return &models.Edition{}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionFunc: func(selector interface{}) (*models.Edition, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})

	Convey("When edition does not exist for a dataset return status not found", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		r.Header.Add("internal_token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionFunc: func(selector interface{}) (*models.Edition, error) {
				return nil, api_errors.EditionNotFound
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})

	Convey("When edition is not published for a dataset return status not found", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionFunc: func(selector interface{}) (*models.Edition, error) {
				return nil, api_errors.EditionNotFound
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
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
			GetVersionsFunc: func(selector interface{}) (*models.VersionResults, error) {
				return &models.VersionResults{}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetVersionsFunc: func(selector interface{}) (*models.VersionResults, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})

	Convey("When version does not exist for an edition of a dataset returns status not found", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		r.Header.Add("internal_token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetVersionsFunc: func(selector interface{}) (*models.VersionResults, error) {
				return nil, api_errors.VersionNotFound
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})

	Convey("When version is not published against an edition of a dataset return status not found", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetVersionsFunc: func(selector interface{}) (*models.VersionResults, error) {
				return nil, api_errors.VersionNotFound
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
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
			GetVersionFunc: func(selector interface{}) (*models.Version, error) {
				return &models.Version{}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetVersionFunc: func(selector interface{}) (*models.Version, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When version does not exist for an edition of a dataset return status not found", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetVersionFunc: func(selector interface{}) (*models.Version, error) {
				return nil, api_errors.VersionNotFound
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When version is not published for an edition of a dataset return status not found", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetVersionFunc: func(selector interface{}) (*models.Version, error) {
				return nil, api_errors.VersionNotFound
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestPostDatasetsReturnsCreated(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		var b string
		b = datasetPayload
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		r.Header.Add("internal_token", "coffee")
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
		r.Header.Add("internal_token", "coffee")
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
		r.Header.Add("internal_token", "coffee")
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

	Convey("When the request does not contain a valid internal token return status unauthorised", t, func() {
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

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})
}

func TestPostEditionReturnsCreated(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		var b string
		b = editionPayload
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017", bytes.NewBufferString(b))
		r.Header.Add("internal_token", "coffee")
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
		r.Header.Add("internal_token", "coffee")
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
		r.Header.Add("internal_token", "coffee")
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

	Convey("When the request does not contain a valid internal token return status unauthorised", t, func() {
		var b string
		b = datasetPayload
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpsertEditionFunc: func(string, interface{}) error {
				return nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
	})
}

func TestPostVersionReturnsCreated(t *testing.T) {
	t.Parallel()
	Convey("When the json body does not contain a state", t, func() {
		var b string
		b = versionPayload
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017/versions", bytes.NewBufferString(b))
		r.Header.Add("internal_token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := setUp("created")

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusCreated)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the json body contains a state of associated", t, func() {
		var b string
		b = versionAssociatedPayload
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017/versions", bytes.NewBufferString(b))
		r.Header.Add("internal_token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := setUp(associatedState)

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusCreated)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

	})

	Convey("When the json body contains a state of published", t, func() {
		var b string
		b = versionPublishedPayload
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017/versions", bytes.NewBufferString(b))
		r.Header.Add("internal_token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := setUp(publishedState)

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusCreated)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
	})
}

func TestPostVersionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		var b string
		b = "{"
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017/versions", bytes.NewBufferString(b))
		r.Header.Add("internal_token", "coffee")
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
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017/versions", bytes.NewBufferString(b))
		r.Header.Add("internal_token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionFunc: func(interface{}) (*models.Edition, error) {
				return nil, internalError
			},
		}
		mockedDataStore.GetEdition(bson.M{"links.dataset.id": "123", "edition": "2017"})

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 2)
	})

	Convey("When the api cannot connect to datastore to update version resource return an internal server error", t, func() {
		var b string
		b = "{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017/versions", bytes.NewBufferString(b))
		r.Header.Add("internal_token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionFunc: func(interface{}) (*models.Edition, error) {
				return &models.Edition{}, nil
			},
			GetNextVersionFunc: func(string, string) (int, error) {
				return 1, nil
			},
			UpsertVersionFunc: func(string, interface{}) error {
				return internalError
			},
		}

		mockedDataStore.GetEdition(bson.M{"links.dataset.id": "123", "edition": "2017"})
		mockedDataStore.GetNextVersion("123", "2017")

		update := bson.M{
			"$set": bson.M{
				"state":         "published",
				"collection_id": "12345",
			},
			"$setOnInsert": bson.M{
				"updated_at": time.Now(),
			},
		}
		mockedDataStore.UpsertVersion("1", update)

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertVersionCalls()), ShouldEqual, 2)
	})

	Convey("When the request does not contain a valid internal token return status unauthorised", t, func() {
		var b string
		b = "{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017/versions", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionFunc: func(interface{}) (*models.Edition, error) {
				return &models.Edition{}, nil
			},
		}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
	})

	Convey("When a request missing the collection_id to create version document to be associated with a collection return status bad request", t, func() {
		var b string
		b = "{\"edition\":\"2017\",\"state\":\"associated\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("POST", "http://localhost:22000/datasets/123/editions/2017/versions", bytes.NewBufferString(b))
		r.Header.Add("internal_token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetEditionFunc: func(interface{}) (*models.Edition, error) {
				return &models.Edition{}, nil
			},
			GetNextVersionFunc: func(string, string) (int, error) {
				return 1, nil
			},
			UpsertVersionFunc: func(string, interface{}) error {
				return nil
			},
		}

		mockedDataStore.GetEdition(bson.M{"links.dataset.id": "123", "edition": "2017"})
		mockedDataStore.GetNextVersion("123", "2017")

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertVersionCalls()), ShouldEqual, 0)
	})
}

func setUp(state string) *backendtest.BackendMock {
	mockedDataStore := &backendtest.BackendMock{
		GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
			return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{}}, nil
		},
		GetEditionFunc: func(interface{}) (*models.Edition, error) {
			return &models.Edition{}, nil
		},
		GetNextVersionFunc: func(string, string) (int, error) {
			return 1, nil
		},
		UpdateDatasetFunc: func(string, interface{}) error {
			return nil
		},
		UpdateEditionFunc: func(string, interface{}) error {
			return nil
		},
		UpsertDatasetFunc: func(string, interface{}) error {
			return nil
		},
		UpsertVersionFunc: func(string, interface{}) error {
			return nil
		},
	}

	selector := bson.M{
		"links.dataset.id": "123",
		"edition":          "2017",
	}
	mockedDataStore.GetEdition(selector)

	mockedDataStore.GetNextVersion("123", "2017")

	update := bson.M{
		"$set": bson.M{
			"state": state,
		},
		"$setOnInsert": bson.M{
			"updated_at": time.Now(),
		},
	}
	mockedDataStore.UpsertVersion("1", update)

	if state == publishedState {
		update := bson.M{
			"$set": bson.M{
				"state": state,
			},
			"$setOnInsert": bson.M{
				"updated_at": time.Now(),
			},
		}
		mockedDataStore.UpdateEdition("123", update)
		mockedDataStore.GetDataset("123")
		mockedDataStore.UpsertDataset("123", update)
	}

	if state == associatedState {
		update := bson.M{
			"$set": bson.M{
				"state": state,
			},
			"$setOnInsert": bson.M{
				"updated_at": time.Now(),
			},
		}
		mockedDataStore.UpdateDataset("123", update)
	}

	return mockedDataStore
}
