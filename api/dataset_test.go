package api

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	host      = "http://localhost:22000"
	secretKey = "coffee"
)

var (
	internalError   = errors.New("internal error")
	badRequestError = errors.New("bad request")
	notFoundError   = errors.New("not found")

	datasetPayload           = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","title":"CensusEthnicity","theme":"population","periodicity":"yearly","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"}}`
	editionPayload           = `{"edition":"2017","state":"created"}`
	versionPayload           = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04"}`
	versionAssociatedPayload = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"associated","collection_id":"12345"}`
	versionPublishedPayload  = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"published","collection_id":"12345"}`
)

func TestGetDatasetsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func() (*models.DatasetResults, error) {
				return &models.DatasetResults{}, nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
	})
}

func TestGetDatasetsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func() (*models.DatasetResults, error) {
				return nil, internalError
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
	})
}

func TestGetDatasetReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("When dataset document has a current sub document return status 200", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Current: &models.Dataset{ID: "123"}}, nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When dataset document has only a next sub document and request contains valid internal_token return status 200", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		r.Header.Add("internal-token", secretKey)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{ID: "123"}}, nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})
}

func TestGetDatasetReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return nil, internalError
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When dataset document has only a next sub document return status 404", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{ID: "123"}}, nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When there is no dataset document return status 404", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return nil, errs.DatasetNotFound
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionsFunc: func(id, state string) (*models.EditionResults, error) {
				return &models.EditionResults{}, nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionsFunc: func(id, state string) (*models.EditionResults, error) {
				return nil, internalError
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})

	Convey("When no editions exist against an existing dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionsFunc: func(id, state string) (*models.EditionResults, error) {
				return nil, errs.EditionNotFound
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})

	Convey("When no published editions exist against a published dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionsFunc: func(id, state string) (*models.EditionResults, error) {
				return nil, errs.EditionNotFound
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(id, editionID, state string) (*models.Edition, error) {
				return &models.Edition{}, nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(id, editionID, state string) (*models.Edition, error) {
				return nil, internalError
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})

	Convey("When edition does not exist for a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(id, editionID, state string) (*models.Edition, error) {
				return nil, errs.EditionNotFound
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})

	Convey("When edition is not published for a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(id, editionID, state string) (*models.Edition, error) {
				return nil, errs.EditionNotFound
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionsFunc: func(datasetID, editionID, state string) (*models.VersionResults, error) {
				return &models.VersionResults{}, nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionsFunc: func(id, editionID, state string) (*models.VersionResults, error) {
				return nil, internalError
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})

	Convey("When version does not exist for an edition of a dataset returns status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionsFunc: func(datasetID, editionID, state string) (*models.VersionResults, error) {
				return nil, errs.VersionNotFound
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})

	Convey("When version is not published against an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionsFunc: func(datasetID, editionID, state string) (*models.VersionResults, error) {
				return nil, errs.VersionNotFound
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{}, nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return nil, internalError
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When version does not exist for an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return nil, errs.VersionNotFound
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When version is not published for an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return nil, errs.VersionNotFound
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
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
		r := httptest.NewRequest("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		r.Header.Add("internal-token", secretKey)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			UpsertDatasetFunc: func(id string, datasetDoc *models.DatasetUpdate) error {
				return nil
			},
		}
		mockedDataStore.UpsertDataset("123", &models.DatasetUpdate{})

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
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
		r := httptest.NewRequest("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		r.Header.Add("internal-token", secretKey)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return badRequestError
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		var b string
		b = datasetPayload
		r := httptest.NewRequest("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		r.Header.Add("internal-token", secretKey)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return internalError
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When the request does not contain a valid internal token return status unauthorised", t, func() {
		var b string
		b = datasetPayload
		r := httptest.NewRequest("POST", "http://localhost:22000/datasets", bytes.NewBufferString(b))
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})
}

func TestPutDatasetReturnsSuccessfully(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		var b string
		b = datasetPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		r.Header.Add("internal-token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			UpdateDatasetFunc: func(string, *models.Dataset) error {
				return nil
			},
		}

		dataset := &models.Dataset{
			Title: "CPI",
		}
		mockedDataStore.UpdateDataset("123", dataset)

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 2)
	})
}

func TestPutDatasetReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		var b string
		b = "{"
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		r.Header.Add("internal-token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			UpdateDatasetFunc: func(string, *models.Dataset) error {
				return badRequestError
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(len(mockedDataStore.UpsertVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		var b string
		b = versionPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		r.Header.Add("internal-token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			UpdateDatasetFunc: func(string, *models.Dataset) error {
				return internalError
			},
		}

		dataset := &models.Dataset{
			Title: "CPI",
		}
		mockedDataStore.UpdateDataset("123", dataset)

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 2)
	})

	Convey("When the dataset document cannot be found return status not found ", t, func() {
		var b string
		b = datasetPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		r.Header.Add("internal-token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			UpdateDatasetFunc: func(string, *models.Dataset) error {
				return errs.DatasetNotFound
			},
		}

		dataset := &models.Dataset{
			Title: "CPI",
		}
		mockedDataStore.UpdateDataset("123", dataset)

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 2)
	})

	Convey("When the request does not contain a valid internal token return status unauthorised", t, func() {
		var b string
		b = "{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			UpdateDatasetFunc: func(string, *models.Dataset) error {
				return nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
	})
}

func TestPutVersionReturnsSuccessfully(t *testing.T) {
	t.Parallel()
	Convey("When state is unchanged", t, func() {
		var b string
		b = versionPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		r.Header.Add("internal-token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					ID:         "789",
					InstanceID: "87654321",
					License:    "ONS License",
					Links: models.VersionLinks{
						Dataset: models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
							ID:   "123",
						},
						Dimensions: models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
						},
						Edition: models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017",
							ID:   "456",
						},
						Self: models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
						},
					},
					ReleaseDate: "2017-12-12",
					State:       "created",
				}, nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
		}
		mockedDataStore.GetVersion("123", "2017", "1", "")
		mockedDataStore.UpdateVersion("a1b2c3", &models.Version{})

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
	})

	Convey("When state is set to associated", t, func() {
		var b string
		b = versionAssociatedPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		r.Header.Add("internal-token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: "associated",
				}, nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
			UpdateDatasetWithAssociationFunc: func(string, string, *models.Version) error {
				return nil
			},
		}
		mockedDataStore.GetVersion("123", "2017", "1", "")
		mockedDataStore.UpdateVersion("a1b2c3", &models.Version{})
		mockedDataStore.UpdateDatasetWithAssociation("123", "associated", &models.Version{})

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When state is set to published", t, func() {
		var b string
		b = versionPublishedPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		r.Header.Add("internal-token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					ID:         "789",
					InstanceID: "87654321",
					License:    "ONS License",
					Links: models.VersionLinks{
						Dataset: models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
							ID:   "123",
						},
						Dimensions: models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
						},
						Edition: models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017",
							ID:   "456",
						},
						Self: models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
						},
					},
					ReleaseDate: "2017-12-12",
					State:       "created",
				}, nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
			UpdateEditionFunc: func(string, string) error {
				return nil
			},
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID:      "123",
					Next:    &models.Dataset{},
					Current: &models.Dataset{},
				}, nil
			},
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return nil
			},
		}
		mockedDataStore.GetVersion("789", "2017", "1", "")
		mockedDataStore.UpdateVersion("a1b2c3", &models.Version{})
		mockedDataStore.UpdateEdition("456", "published")
		mockedDataStore.GetDataset("123")
		mockedDataStore.UpsertDataset("123", &models.DatasetUpdate{})

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
	})
}

func TestPutVersionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		var b string
		b = "{"
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		r.Header.Add("internal-token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{}, nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		var b string
		b = versionPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		r.Header.Add("internal-token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return nil, internalError
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the version document cannot be found return status not found", t, func() {
		var b string
		b = versionPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		r.Header.Add("internal-token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return nil, errs.VersionNotFound
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the request does not contain a valid internal token return status unauthorised", t, func() {
		var b string
		b = versionPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{}, nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the version document has already been published return status forbidden", t, func() {
		var b string
		b = versionPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		r.Header.Add("internal-token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: "published",
				}, nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the request body is invalid return status not found", t, func() {
		var b string
		b = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"associated"}`
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		r.Header.Add("internal-token", "coffee")
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{}, nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
	})
}

func TestGetDimensionsReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("When the request contain valid ids return dimension information", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDimensionsFunc: func(datasetID, editionID, versionID string) (*models.DatasetDimensionResults, error) {
				return &models.DatasetDimensionResults{}, nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 1)
	})
}

func TestGetDimensionsReturnsErrors(t *testing.T) {
	t.Parallel()
	Convey("When the request contain invalid ids return not found error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDimensionsFunc: func(datasetID, editionID, versionID string) (*models.DatasetDimensionResults, error) {
				return nil, errs.VersionNotFound
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
	})

	Convey("When the api cannot connect to datastore to get dimension resource return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDimensionsFunc: func(datasetID, editionID, versionID string) (*models.DatasetDimensionResults, error) {
				return nil, internalError
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
	})
}

func TestGetDimensionOptionsReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDimensionOptionsFunc: func(datasetID, editionID, versionID, dimensions string) (*models.DimensionOptionResults, error) {
				return &models.DimensionOptionResults{}, nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
	})
}

func TestGetDimensionOptionsReturnsErrors(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDimensionOptionsFunc: func(datasetID, editionID, versionID, dimensions string) (*models.DimensionOptionResults, error) {
				return nil, errs.DatasetNotFound
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
	})

	Convey("", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDimensionOptionsFunc: func(datasetID, editionID, versionID, dimensions string) (*models.DimensionOptionResults, error) {
				return nil, internalError
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
	})
}

func TestCreateNewVersionDoc(t *testing.T) {
	t.Parallel()
	Convey("Check the version has the new collection id when request contains a collection_id", t, func() {
		currentVersion := &models.Version{}
		version := &models.Version{
			CollectionID: "4321",
		}

		newVersion := createNewVersionDoc(currentVersion, version)
		So(newVersion.CollectionID, ShouldNotBeNil)
		So(newVersion.CollectionID, ShouldEqual, "4321")
	})

	Convey("Check the version collection id does not get replaced by the current collection id when request contains a collection_id", t, func() {
		currentVersion := &models.Version{
			CollectionID: "1234",
		}
		version := &models.Version{
			CollectionID: "4321",
		}

		newVersion := createNewVersionDoc(currentVersion, version)
		So(newVersion.CollectionID, ShouldNotBeNil)
		So(newVersion.CollectionID, ShouldEqual, "4321")
	})

	Convey("Check the version has the old collection id when request is missing a collection_id", t, func() {
		currentVersion := &models.Version{
			CollectionID: "1234",
		}
		version := &models.Version{}

		newVersion := createNewVersionDoc(currentVersion, version)
		So(newVersion.CollectionID, ShouldNotBeNil)
		So(newVersion.CollectionID, ShouldEqual, "1234")
	})

	Convey("check the version collection id is not set when both request body and current version document are missing a collection id", t, func() {
		currentVersion := &models.Version{}
		version := &models.Version{}

		newVersion := createNewVersionDoc(currentVersion, version)
		So(newVersion.CollectionID, ShouldNotBeNil)
		So(newVersion.CollectionID, ShouldEqual, "")
	})
}

func setUp(state string) *storetest.StorerMock {
	mockedDataStore := &storetest.StorerMock{
		GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
			return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{}}, nil
		},
		GetEditionFunc: func(string, string, string) (*models.Edition, error) {
			return &models.Edition{}, nil
		},
		GetNextVersionFunc: func(string, string) (int, error) {
			return 1, nil
		},
		UpdateDatasetWithAssociationFunc: func(string, string, *models.Version) error {
			return nil
		},
		UpdateEditionFunc: func(string, string) error {
			return nil
		},
		UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
			return nil
		},
		UpsertVersionFunc: func(string, *models.Version) error {
			return nil
		},
	}

	mockedDataStore.GetEdition("123", "2017", state)

	mockedDataStore.GetNextVersion("123", "2017")

	versionDoc := &models.Version{
		State: state,
	}
	mockedDataStore.UpsertVersion("1", versionDoc)

	if state == publishedState {
		datasetDoc := &models.DatasetUpdate{
			ID: "123",
			Next: &models.Dataset{
				State: state,
			},
		}
		mockedDataStore.UpdateEdition("123", state)
		mockedDataStore.GetDataset("123")
		mockedDataStore.UpsertDataset("123", datasetDoc)
	}

	if state == associatedState {
		versionDoc := &models.Version{
			State: state,
		}
		mockedDataStore.UpdateDatasetWithAssociation("123", state, versionDoc)
	}

	return mockedDataStore
}
