package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/url"
	dprequest "github.com/ONSdigital/dp-net/request"
	"github.com/gorilla/mux"

	"github.com/ONSdigital/dp-dataset-api/config"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	host              = "http://localhost:22000"
	authToken         = "dataset"
	healthTimeout     = 2 * time.Second
	internalServerErr = "internal server error\n"
	callerIdentity    = "someone@ons.gov.uk"
)

var (
	datasetPayload = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"nomis","nomis_reference_url":"https://www.nomis.co.uk"}`
	urlBuilder     = url.NewBuilder("localhost:20000")
	mu             sync.Mutex
)

func getAuthorisationHandlerMock() *mocks.AuthHandlerMock {
	return &mocks.AuthHandlerMock{
		Required: &mocks.PermissionCheckCalls{Calls: 0},
	}
}

// GetAPIWithMocks also used in other tests, so exported
func GetAPIWithMocks(mockedDataStore store.Storer, mockedGeneratedDownloads DownloadsGenerator, datasetPermissions AuthHandler, permissions AuthHandler) *DatasetAPI {
	mu.Lock()
	defer mu.Unlock()
	cfg, err := config.Get()
	So(err, ShouldBeNil)
	cfg.ServiceAuthToken = authToken
	cfg.DatasetAPIURL = host
	cfg.EnablePrivateEndpoints = true
	cfg.MongoConfig.Limit = 0
	cfg.MongoConfig.Offset = 0

	return Setup(testContext, cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedGeneratedDownloads, datasetPermissions, permissions)
}

func createRequestWithAuth(method, URL string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequest(method, URL, body)
	ctx := r.Context()
	ctx = dprequest.SetCaller(ctx, "someone@ons.gov.uk")
	r = r.WithContext(ctx)
	return r, err
}

func TestGetDatasetsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("A successful request to get dataset returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func(ctx context.Context, offset, limit int, authorised bool) (*models.DatasetUpdateResults, error) {
				return &models.DatasetUpdateResults{}, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
		So(datasetPermissions.Required.Calls, ShouldEqual, 0)
		So(permissions.Required.Calls, ShouldEqual, 1)
	})
}

func TestGetDatasetsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func(ctx context.Context, offset, limit int, authorised bool) (*models.DatasetUpdateResults, error) {
				return nil, errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
		So(datasetPermissions.Required.Calls, ShouldEqual, 0)
		So(permissions.Required.Calls, ShouldEqual, 1)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When dataset document has only a next sub document and request is authorised return status 200", t, func() {
		r, err := createRequestWithAuth("GET", "http://localhost:22000/datasets/123-456", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{ID: "123"}}, nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
	})
}

func TestGetDatasetReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When there is no dataset document return status 404", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})
}

func TestPostDatasetsReturnsCreated(t *testing.T) {
	t.Parallel()
	Convey("A successful request to post dataset returns 200 OK response", t, func() {
		var b string
		b = datasetPayload
		r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(id string, datasetDoc *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		mockedDataStore.UpsertDataset("123", &models.DatasetUpdate{Next: &models.Dataset{}})
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 2)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})
}

func TestPostDatasetReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		var b string
		b = "{"
		r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return errs.ErrAddUpdateDatasetBadRequest
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		var b string
		b = datasetPayload
		r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrInternalServer
			},
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request does not contain a valid internal token returns 401", t, func() {
		var b string
		b = datasetPayload
		r := httptest.NewRequest("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return nil
			},
		}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(datasetPermissions.Required.Calls, ShouldEqual, 0)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err := r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the dataset already exists and a request is sent to create the same dataset return status forbidden", t, func() {
		var b string
		b = datasetPayload
		r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
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

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldResemble, "forbidden - dataset already exists\n")
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request has an filterable datatype and nomis url it should return type mismatch error", t, func() {
		var b string
		b = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"filterable","nomis_reference_url":"https://www.nomis.co.uk"}`

		r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Body.String(), ShouldResemble, "type mismatch\n")
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 0)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request has an invalid datatype it should return invalid type errorq", t, func() {
		var b string
		b = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"nomis_filterable"}`

		r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldResemble, "invalid dataset type\n")
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 0)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request body has an empty type field it should create a dataset with type defaulted to filterable", t, func() {
		var b string
		b = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":""}`
		res := `{"id":"123123","next":{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","id":"123123","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"},"editions":{"href":"http://localhost:22000/datasets/123123/editions"},"self":{"href":"http://localhost:22000/datasets/123123"}},"next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department"},"state":"created","theme":"population","title":"CensusEthnicity","type":"filterable"}}`
		r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		mockedDataStore.UpsertDataset("123123", &models.DatasetUpdate{Next: &models.Dataset{}})
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(w.Body.String(), ShouldContainSubstring, res)
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpsertDatasetCalls(), ShouldHaveLength, 2)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

}

func TestPutDatasetReturnsSuccessfully(t *testing.T) {
	t.Parallel()
	Convey("A successful request to put dataset returns 200 OK response", t, func() {
		var b string
		b = datasetPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "nomis"}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		dataset := &models.Dataset{
			Title: "CPI",
		}
		mockedDataStore.UpdateDataset(testContext, "123", dataset, models.CreatedState)

		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 2)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When update dataset type has a value of filterable and stored dataset type is nomis return status ok", t, func() {
		// Dataset type field cannot be updated and hence is ignored in any updates to the dataset

		var b string
		b = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"type":"filterable","nomis_reference_url":"https://www.nomis.co.uk"}`

		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "nomis"}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 1)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})
}

func TestPutDatasetReturnsError(t *testing.T) {

	t.Parallel()
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		var b string
		b = "{"
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return errs.ErrAddUpdateDatasetBadRequest
			},
		}

		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		var b string
		b = versionPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: models.CreatedState}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return errs.ErrInternalServer
			},
		}

		dataset := &models.Dataset{
			Title: "CPI",
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		mockedDataStore.UpdateDataset(testContext, "123", dataset, models.CreatedState)
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 2)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the dataset document cannot be found return status not found ", t, func() {
		var b string
		b = datasetPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return errs.ErrDatasetNotFound
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When updating the dataset nomis_reference_url and the stored dataset type is not nomis return bad request", t, func() {
		var b string
		b = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"},"nomis_reference_url":"https://www.nomis.co.uk"}`

		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{Type: "filterable"}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()

		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldResemble, errs.ErrTypeMismatch.Error()+"\n")
		So(mockedDataStore.GetDatasetCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateDatasetCalls(), ShouldHaveLength, 0)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})

	Convey("When the request is not authorised to update dataset return status unauthorised", t, func() {
		var b string
		b = "{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{}}, nil
			},
			UpdateDatasetFunc: func(context.Context, string, *models.Dataset, string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(datasetPermissions.Required.Calls, ShouldEqual, 0)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})
}

func TestDeleteDatasetReturnsSuccessfully(t *testing.T) {
	t.Parallel()
	Convey("A successful request to delete dataset returns 200 OK response", t, func() {
		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: models.CreatedState}}, nil
			},
			GetEditionsFunc: func(ctx context.Context, ID string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)
	})

	Convey("A successful request to delete dataset with editions returns 200 OK response", t, func() {
		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: models.CreatedState}}, nil
			},
			GetEditionsFunc: func(ctx context.Context, ID string, state string) (*models.EditionUpdateResults, error) {
				var items []*models.EditionUpdate
				items = append(items, &models.EditionUpdate{})
				return &models.EditionUpdateResults{Items: items}, nil
			},
			DeleteEditionFunc: func(ID string) error {
				return nil
			},
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)
	})
}

func TestDeleteDatasetReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When a request to delete a published dataset return status forbidden", t, func() {
		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
			},
			GetEditionsFunc: func(ctx context.Context, ID string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: models.CreatedState}}, nil
			},
			GetEditionsFunc: func(ctx context.Context, ID string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
			DeleteDatasetFunc: func(string) error {
				return errs.ErrInternalServer
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When the dataset document cannot be found return status not found ", t, func() {
		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			GetEditionsFunc: func(ctx context.Context, ID string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset document cannot be queried return status 500 ", t, func() {
		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return nil, errors.New("database is broken")
			},
			GetEditionsFunc: func(ctx context.Context, ID string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(datasetPermissions.Required.Calls, ShouldEqual, 1)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the request is not authorised to delete the dataset return status not found", t, func() {
		var b string
		b = "{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("DELETE", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}
		datasetPermissions := getAuthorisationHandlerMock()
		permissions := getAuthorisationHandlerMock()
		api := GetAPIWithMocks(mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(datasetPermissions.Required.Calls, ShouldEqual, 0)
		So(permissions.Required.Calls, ShouldEqual, 0)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)
	})
}
