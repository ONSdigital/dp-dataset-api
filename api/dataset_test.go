package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/audit/auditortest"
	"github.com/ONSdigital/go-ns/common"
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
	datasetPayload = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","periodicity":"yearly","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"}}`

	urlBuilder         = url.NewBuilder("localhost:20000")
	genericAuditParams = common.Params{"caller_identity": callerIdentity, "dataset_id": "123-456"}
)

func getAuthorisationHandlerMock() *mocks.AuthHandlerMock {
	return &mocks.AuthHandlerMock{
		CheckPermissions: &mocks.PermissionCheckCalls{InvocationCount: 0},
	}
}

// GetAPIWithMockedDatastore also used in other tests, so exported
func GetAPIWithMockedDatastore(mockedDataStore store.Storer, mockedGeneratedDownloads DownloadsGenerator, auditMock Auditor, authHandler AuthHandler) *DatasetAPI {
	cfg, err := config.Get()
	So(err, ShouldBeNil)
	cfg.ServiceAuthToken = authToken
	cfg.DatasetAPIURL = host
	cfg.EnablePrivateEnpoints = true

	return NewDatasetAPI(*cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedGeneratedDownloads, auditMock, authHandler)
}

func createRequestWithAuth(method, URL string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequest(method, URL, body)
	ctx := r.Context()
	ctx = common.SetCaller(ctx, "someone@ons.gov.uk")
	r = r.WithContext(ctx)
	return r, err
}

func TestGetDatasetsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("A successful request to get dataset returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func() ([]models.DatasetUpdate, error) {
				return []models.DatasetUpdate{}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: getDatasetsAction, Result: audit.Attempted, Params: nil},
			auditortest.Expected{Action: getDatasetsAction, Result: audit.Successful, Params: nil},
		)
	})
}

func TestGetDatasetsReturnsErrorIfAuditAttemptFails(t *testing.T) {
	t.Parallel()
	Convey("When auditing get datasets attempt returns an error an internal server error is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func() ([]models.DatasetUpdate, error) {
				return nil, errs.ErrInternalServer
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return errors.New("boom!")
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{
				Action: getDatasetsAction,
				Result: audit.Attempted,
				Params: nil,
			},
		)
	})

	Convey("When auditing get datasets errors an internal server error is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func() ([]models.DatasetUpdate, error) {
				return nil, errs.ErrInternalServer
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getDatasetsAction && result == audit.Unsuccessful {
				return errors.New("boom!")
			}
			return nil
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: getDatasetsAction, Result: audit.Attempted, Params: nil},
			auditortest.Expected{Action: getDatasetsAction, Result: audit.Unsuccessful, Params: nil},
		)
	})
}

func TestGetDatasetsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func() ([]models.DatasetUpdate, error) {
				return nil, errs.ErrInternalServer
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: getDatasetsAction, Result: audit.Attempted, Params: nil},
			auditortest.Expected{Action: getDatasetsAction, Result: audit.Unsuccessful, Params: nil},
		)
	})
}

func TestGetDatasetsAuditSuccessfulError(t *testing.T) {
	t.Parallel()
	Convey("when a successful request to get dataset fails to audit action successful then a 500 response is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func() ([]models.DatasetUpdate, error) {
				return []models.DatasetUpdate{}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.NewErroring(getDatasetsAction, audit.Successful)
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: getDatasetsAction, Result: audit.Attempted, Params: nil},
			auditortest.Expected{Action: getDatasetsAction, Result: audit.Successful, Params: nil},
		)
	})
}

func TestGetDatasetReturnsOK(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123-456"}

	t.Parallel()
	Convey("When dataset document has a current sub document return status 200", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Current: &models.Dataset{ID: "123"}}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getDatasetAction, Result: audit.Successful, Params: auditParams},
		)
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

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getDatasetAction, Result: audit.Successful, Params: auditParams},
		)
	})
}

func TestGetDatasetReturnsError(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123-456"}

	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrInternalServer
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getDatasetAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})

	Convey("When dataset document has only a next sub document return status 404", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{ID: "123"}}, nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getDatasetAction, Result: audit.Unsuccessful, Params: auditParams},
		)
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

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: getDatasetAction, Result: audit.Unsuccessful, Params: auditParams},
		)
	})
}

func TestGetDatasetAuditingErrors(t *testing.T) {
	auditParams := common.Params{"dataset_id": "123-456"}

	Convey("given auditing attempted action returns an error", t, func() {
		auditMock := auditortest.New()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return errors.New("auditing error")
		}

		Convey("when get dataset is called", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
			w := httptest.NewRecorder()

			authHandler := getAuthorisationHandlerMock()
			mockDatastore := &storetest.StorerMock{}
			api := GetAPIWithMockedDatastore(mockDatastore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)

			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				assertInternalServerErr(w)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockDatastore.GetDatasetCalls()), ShouldEqual, 0)

				auditMock.AssertRecordCalls(
					auditortest.Expected{
						Action: getDatasetAction,
						Result: audit.Attempted,
						Params: auditParams,
					},
				)
			})
		})
	})

	Convey("given audit action successful returns an error", t, func() {
		auditMock := auditortest.New()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getDatasetAction && result == audit.Successful {
				return errors.New("auditing error")
			}
			return nil
		}

		Convey("when get dataset is called", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
			w := httptest.NewRecorder()

			mockDatastore := &storetest.StorerMock{
				GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{ID: "123", Current: &models.Dataset{ID: "123"}}, nil
				},
			}

			authHandler := getAuthorisationHandlerMock()

			api := GetAPIWithMockedDatastore(mockDatastore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(len(mockDatastore.GetDatasetCalls()), ShouldEqual, 1)
				assertInternalServerErr(w)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: auditParams},
					auditortest.Expected{Action: getDatasetAction, Result: audit.Successful, Params: auditParams},
				)
			})
		})
	})

	Convey("given auditing action unsuccessful returns an error", t, func() {
		auditMock := auditortest.New()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getDatasetAction && result == audit.Unsuccessful {
				return errors.New("auditing error")
			}
			return nil
		}

		Convey("when get dataset is called", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
			w := httptest.NewRecorder()

			mockDatastore := &storetest.StorerMock{
				GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
					return nil, errors.New("get dataset error")
				},
			}

			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(mockDatastore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				assertInternalServerErr(w)
				So(len(mockDatastore.GetDatasetCalls()), ShouldEqual, 1)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: auditParams},
					auditortest.Expected{Action: getDatasetAction, Result: audit.Unsuccessful, Params: auditParams},
				)
			})
		})
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

		authHandler := getAuthorisationHandlerMock()
		mockedDataStore.UpsertDataset("123", &models.DatasetUpdate{Next: &models.Dataset{}})
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 2)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: addDatasetAction, Result: audit.Successful, Params: common.Params{"dataset_id": "123"}},
		)

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

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: addDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
		)

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

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: addDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
		)

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
		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

		auditParams := common.Params{"dataset_id": "123"}
		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: auditParams},
			auditortest.Expected{Action: addDatasetAction, Result: audit.Unsuccessful, Params: auditParams},
		)

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

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(w.Body.String(), ShouldResemble, "forbidden - dataset already exists\n")
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: addDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})
}

func TestPostDatasetAuditErrors(t *testing.T) {
	Convey("given audit action attempted returns an error", t, func() {
		auditMock := auditortest.NewErroring(addDatasetAction, audit.Attempted)

		Convey("when add dataset is called", func() {
			r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString("{"))
			So(err, ShouldBeNil)

			authHandler := getAuthorisationHandlerMock()
			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				assertInternalServerErr(w)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

				auditMock.AssertRecordCalls(
					auditortest.Expected{
						Action: addDatasetAction,
						Result: audit.Attempted,
						Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"},
					},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})

	Convey("given audit action unsuccessful returns an error", t, func() {
		auditMock := auditortest.NewErroring(addDatasetAction, audit.Unsuccessful)

		Convey("when datastore getdataset returns an error", func() {
			r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString("{"))
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			authHandler := getAuthorisationHandlerMock()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return nil, errors.New("get dataset error")
				},
			}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				assertInternalServerErr(w)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: addDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})

		Convey("when datastore getdataset returns an existing dataset", func() {
			r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString("{"))
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{}, nil
				},
			}

			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 403 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrAddDatasetAlreadyExists.Error())
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: addDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})

		Convey("when datastore upsertDataset returns error", func() {
			r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(datasetPayload))
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return nil, errs.ErrDatasetNotFound
				},
				UpsertDatasetFunc: func(ID string, datasetDoc *models.DatasetUpdate) error {
					return errors.New("upsert datset error")
				},
			}

			authHandler := getAuthorisationHandlerMock()

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				assertInternalServerErr(w)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: addDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})

	Convey("given audit action successful returns an error", t, func() {
		auditMock := auditortest.NewErroring(addDatasetAction, audit.Successful)

		Convey("when add dataset is successful", func() {
			r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(datasetPayload))
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return nil, errs.ErrDatasetNotFound
				},
				UpsertDatasetFunc: func(ID string, datasetDoc *models.DatasetUpdate) error {
					return nil
				},
			}
			authHandler := getAuthorisationHandlerMock()

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 201 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusCreated)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: addDatasetAction, Result: audit.Successful, Params: common.Params{"dataset_id": "123"}},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
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
				return &models.DatasetUpdate{Next: &models.Dataset{}}, nil
			},
			UpdateDatasetFunc: func(string, *models.Dataset, string) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()

		dataset := &models.Dataset{
			Title: "CPI",
		}
		mockedDataStore.UpdateDataset("123", dataset, models.CreatedState)

		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 2)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: updateDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: updateDatasetAction, Result: audit.Successful, Params: common.Params{"dataset_id": "123"}},
		)

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

		authHandler := getAuthorisationHandlerMock()

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{}}, nil
			},
			UpdateDatasetFunc: func(string, *models.Dataset, string) error {
				return errs.ErrAddUpdateDatasetBadRequest
			},
		}

		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: updateDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: updateDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
		)

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
			UpdateDatasetFunc: func(string, *models.Dataset, string) error {
				return errs.ErrInternalServer
			},
		}

		dataset := &models.Dataset{
			Title: "CPI",
		}

		authHandler := getAuthorisationHandlerMock()
		mockedDataStore.UpdateDataset("123", dataset, models.CreatedState)
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 2)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: updateDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: updateDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
		)

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
			UpdateDatasetFunc: func(string, *models.Dataset, string) error {
				return errs.ErrDatasetNotFound
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDatasetNotFound.Error())

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: updateDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: updateDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
		)

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
			UpdateDatasetFunc: func(string, *models.Dataset, string) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: updateDatasetAction, Result: audit.Attempted, Params: common.Params{"dataset_id": "123"}},
			auditortest.Expected{Action: updateDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
		)

		Convey("then the request body has been drained", func() {
			_, err = r.Body.Read(make([]byte, 1))
			So(err, ShouldEqual, io.EOF)
		})
	})
}

func TestPutDatasetAuditErrors(t *testing.T) {

	t.Parallel()
	Convey("given audit action attempted returns an error", t, func() {
		auditMock := auditortest.NewErroring(updateDatasetAction, audit.Attempted)

		Convey("when put dataset is called", func() {
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(datasetPayload))
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{Next: &models.Dataset{}}, nil
				},
				UpdateDatasetFunc: func(string, *models.Dataset, string) error {
					return nil
				},
			}

			authHandler := getAuthorisationHandlerMock()

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

				auditMock.AssertRecordCalls(
					auditortest.Expected{
						Action: updateDatasetAction,
						Result: audit.Attempted,
						Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"},
					},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})

	Convey("given audit action successful returns an error", t, func() {
		auditMock := auditortest.NewErroring(updateDatasetAction, audit.Successful)

		Convey("when a put dataset request is successful", func() {
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(datasetPayload))
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{Next: &models.Dataset{}}, nil
				},
				UpdateDatasetFunc: func(string, *models.Dataset, string) error {
					return nil
				},
			}

			authHandler := getAuthorisationHandlerMock()

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 200 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 1)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: updateDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: updateDatasetAction, Result: audit.Successful, Params: common.Params{"dataset_id": "123"}},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})
	})

	Convey("given audit action unsuccessful returns an error", t, func() {
		auditMock := auditortest.NewErroring(updateDatasetAction, audit.Unsuccessful)

		Convey("when a put dataset request contains an invalid dataset body", func() {
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString("`zxcvbnm,./"))
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{Next: &models.Dataset{}}, nil
				},
				UpdateDatasetFunc: func(string, *models.Dataset, string) error {
					return nil
				},
			}

			authHandler := getAuthorisationHandlerMock()

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 400 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: updateDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: updateDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})

		Convey("when datastore.getDataset returns an error", func() {
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(datasetPayload))
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return nil, errs.ErrDatasetNotFound
				},
			}

			authHandler := getAuthorisationHandlerMock()

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 400 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: updateDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: updateDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})

		Convey("when the requested dataset has a published state", func() {

			var publishedDataset models.Dataset
			json.Unmarshal([]byte(datasetPayload), &publishedDataset)
			publishedDataset.State = models.PublishedState
			b, _ := json.Marshal(publishedDataset)

			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBuffer(b))
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{Next: &models.Dataset{}}, nil
				},
				UpsertDatasetFunc: func(ID string, datasetDoc *models.DatasetUpdate) error {
					return errors.New("upsertDataset error")
				},
			}

			authHandler := getAuthorisationHandlerMock()

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 400 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: updateDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: updateDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
		})

		Convey("when datastore.UpdateDataset returns an error", func() {
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(datasetPayload))
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{Next: &models.Dataset{}}, nil
				},
				UpdateDatasetFunc: func(ID string, dataset *models.Dataset, currentState string) error {
					return errors.New("update dataset error")
				},
			}

			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 400 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 1)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: updateDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: updateDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
				)
			})

			Convey("then the request body has been drained", func() {
				_, err = r.Body.Read(make([]byte, 1))
				So(err, ShouldEqual, io.EOF)
			})
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
			GetEditionsFunc: func(ID string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Successful, Params: common.Params{"dataset_id": "123"}},
		)
	})

	Convey("A successful request to delete dataset with editions returns 200 OK response", t, func() {
		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: models.CreatedState}}, nil
			},
			GetEditionsFunc: func(ID string, state string) (*models.EditionUpdateResults, error) {
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

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Successful, Params: common.Params{"dataset_id": "123"}},
		)
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
			GetEditionsFunc: func(ID string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
		)
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: models.CreatedState}}, nil
			},
			GetEditionsFunc: func(ID string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
			DeleteDatasetFunc: func(string) error {
				return errs.ErrInternalServer
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
		)
	})

	Convey("When the dataset document cannot be found return status not found ", t, func() {
		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			GetEditionsFunc: func(ID string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
		)
	})

	Convey("When the dataset document cannot be queried return status 500 ", t, func() {
		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return nil, errors.New("database is broken")
			},
			GetEditionsFunc: func(ID string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
		)
	})

	Convey("When the request is not authorised to delete the dataset return status not found", t, func() {
		var b string
		b = "{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("DELETE", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}
		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)

		auditMock.AssertRecordCalls(
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: common.Params{"dataset_id": "123"}},
			auditortest.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
		)
	})

}

func TestDeleteDatasetAuditActionAttemptedError(t *testing.T) {
	t.Parallel()
	Convey("given audit action attempted returns an error", t, func() {
		auditMock := auditortest.NewErroring(deleteDatasetAction, audit.Attempted)

		Convey("when delete dataset is called", func() {
			r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{}
			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 0)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)

				auditMock.AssertRecordCalls(
					auditortest.Expected{
						Action: deleteDatasetAction,
						Result: audit.Attempted,
						Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"},
					},
				)
			})
		})
	})
}

func TestDeleteDatasetAuditauditUnsuccessfulError(t *testing.T) {
	Convey("given auditing action unsuccessful returns an errors", t, func() {
		auditMock := auditortest.NewErroring(deleteDatasetAction, audit.Unsuccessful)

		Convey("when attempting to delete a dataset that does not exist", func() {

			r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return nil, errs.ErrDatasetNotFound
				},
			}
			authHandler := getAuthorisationHandlerMock()
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 204 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNoContent)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
				)
			})
		})

		Convey("when dataStore.Backend.GetDataset returns an error", func() {
			r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return nil, errors.New("dataStore.Backend.GetDataset error")
				},
			}

			authHandler := getAuthorisationHandlerMock()
			auditMock = auditortest.NewErroring(deleteDatasetAction, audit.Unsuccessful)
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				assertInternalServerErr(w)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
				)
			})
		})

		Convey("when attempting to delete a published dataset", func() {
			r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
				},
			}

			authHandler := getAuthorisationHandlerMock()
			auditMock = auditortest.NewErroring(deleteDatasetAction, audit.Unsuccessful)
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 403 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
				)
			})
		})

		Convey("when dataStore.Backend.DeleteEdition returns an error", func() {
			r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{Next: &models.Dataset{State: models.CompletedState}}, nil
				},
				GetEditionsFunc: func(ID string, state string) (*models.EditionUpdateResults, error) {
					var items []*models.EditionUpdate
					items = append(items, &models.EditionUpdate{})
					return &models.EditionUpdateResults{Items: items}, nil
				},
				DeleteEditionFunc: func(ID string) error {
					return errors.New("DeleteEditionFunc error")
				},
			}
			authHandler := getAuthorisationHandlerMock()

			auditMock = auditortest.NewErroring(deleteDatasetAction, audit.Unsuccessful)
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				assertInternalServerErr(w)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
				)
			})
		})

		Convey("when dataStore.Backend.DeleteDataset returns an error", func() {
			r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{Next: &models.Dataset{State: models.CompletedState}}, nil
				},
				GetEditionsFunc: func(ID string, state string) (*models.EditionUpdateResults, error) {
					return &models.EditionUpdateResults{}, nil
				},
				DeleteDatasetFunc: func(ID string) error {
					return errors.New("DeleteDatasetFunc error")
				},
			}
			authHandler := getAuthorisationHandlerMock()
			auditMock = auditortest.NewErroring(deleteDatasetAction, audit.Unsuccessful)
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				assertInternalServerErr(w)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: common.Params{"dataset_id": "123"}},
				)
			})
		})
	})
}

func TestDeleteDatasetAuditActionSuccessfulError(t *testing.T) {
	Convey("given audit action successful returns an error", t, func() {
		r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{State: models.CreatedState}}, nil
			},
			GetEditionsFunc: func(ID string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		authHandler := getAuthorisationHandlerMock()
		auditMock := auditortest.NewErroring(deleteDatasetAction, audit.Successful)

		Convey("when delete dataset is called", func() {
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, authHandler)
			api.Router.ServeHTTP(w, r)

			Convey("then a 204 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNoContent)
				So(authHandler.CheckPermissions.InvocationCount, ShouldEqual, 1)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.DeleteEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)

				auditMock.AssertRecordCalls(
					auditortest.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: common.Params{"caller_identity": "someone@ons.gov.uk", "dataset_id": "123"}},
					auditortest.Expected{Action: deleteDatasetAction, Result: audit.Successful, Params: common.Params{"dataset_id": "123"}},
				)
			})
		})
	})
}
