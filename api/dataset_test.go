package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/audit/audit_mock"
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
)

var (
	errInternal   = errors.New("internal error")
	errBadRequest = errors.New("bad request")
	errNotFound   = errors.New("not found")

	datasetPayload = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","periodicity":"yearly","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"}}`

	urlBuilder                    = url.NewBuilder("localhost:20000")
	genericMockedObservationStore = &mocks.ObservationStoreMock{}
	genericAuditParams            = common.Params{"dataset_id": "123-456"}
)

// GetAPIWithMockedDatastore also used in other tests, so exported
func GetAPIWithMockedDatastore(mockedDataStore store.Storer, mockedGeneratedDownloads DownloadsGenerator, mockAuditor Auditor, mockedObservationStore ObservationStore) *DatasetAPI {
	cfg, err := config.Get()
	So(err, ShouldBeNil)
	cfg.ServiceAuthToken = authToken
	cfg.DatasetAPIURL = host
	cfg.EnablePrivateEnpoints = true
	cfg.HealthCheckTimeout = healthTimeout

	return Routes(*cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedGeneratedDownloads, mockAuditor, mockedObservationStore)
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

		mockAuditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, mockAuditor, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
		mockAuditor.AssertRecordCalls(
			audit_mock.Expected{Action: getDatasetsAction, Result: audit.Attempted, Params: nil},
			audit_mock.Expected{Action: getDatasetsAction, Result: audit.Successful, Params: nil},
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
				return nil, errInternal
			},
		}

		auditMock := audit_mock.New()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return errors.New("boom!")
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		assertInternalServerErr(w)
		auditMock.AssertRecordCalls(audit_mock.Expected{Action: getDatasetsAction, Result: audit.Attempted, Params: nil})
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 0)
	})

	Convey("When auditing get datasets errors an internal server error is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func() ([]models.DatasetUpdate, error) {
				return nil, errInternal
			},
		}

		auditMock := audit_mock.New()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getDatasetsAction && result == audit.Unsuccessful {
				return errors.New("boom!")
			}
			return nil
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(strings.TrimSpace(w.Body.String()), ShouldEqual, errInternal.Error())
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
		auditMock.AssertRecordCalls(
			audit_mock.Expected{Action: getDatasetsAction, Result: audit.Attempted, Params: nil},
			audit_mock.Expected{Action: getDatasetsAction, Result: audit.Unsuccessful, Params: nil},
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
				return nil, errInternal
			},
		}

		auditMock := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
		auditMock.AssertRecordCalls(
			audit_mock.Expected{Action: getDatasetsAction, Result: audit.Attempted, Params: nil},
			audit_mock.Expected{Action: getDatasetsAction, Result: audit.Unsuccessful, Params: nil},
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

		mockAuditor := audit_mock.NewErroring(getDatasetsAction, audit.Successful)

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, mockAuditor, genericMockedObservationStore)
		api.Router.ServeHTTP(w, r)

		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
		mockAuditor.AssertRecordCalls(
			audit_mock.Expected{Action: getDatasetsAction, Result: audit.Attempted, Params: nil},
			audit_mock.Expected{Action: getDatasetsAction, Result: audit.Successful, Params: nil},
		)
		assertInternalServerErr(w)
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

		auditMock := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		auditMock.AssertRecordCalls(
			audit_mock.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: genericAuditParams},
			audit_mock.Expected{Action: getDatasetAction, Result: audit.Successful, Params: genericAuditParams},
		)

		So(w.Code, ShouldEqual, http.StatusOK)
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

		auditMock := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		auditMock.AssertRecordCalls(
			audit_mock.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: genericAuditParams},
			audit_mock.Expected{Action: getDatasetAction, Result: audit.Successful, Params: genericAuditParams},
		)
	})
}

func TestGetDatasetReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return nil, errInternal
			},
		}

		auditMock := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		auditMock.AssertRecordCalls(
			audit_mock.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: genericAuditParams},
			audit_mock.Expected{Action: getDatasetAction, Result: audit.Unsuccessful, Params: genericAuditParams},
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, audit_mock.New(), genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
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

		auditMock := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		auditMock.AssertRecordCalls(
			audit_mock.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: genericAuditParams},
			audit_mock.Expected{Action: getDatasetAction, Result: audit.Unsuccessful, Params: genericAuditParams},
		)
	})
}

func TestGetDatasetAuditingErrors(t *testing.T) {
	Convey("given auditing attempted action returns an error", t, func() {
		auditMock := audit_mock.New()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return errors.New("auditing error")
		}

		Convey("when get dataset is called", func() {
			r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
			w := httptest.NewRecorder()

			mockDatastore := &storetest.StorerMock{}
			api := GetAPIWithMockedDatastore(mockDatastore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				assertInternalServerErr(w)
				So(len(mockDatastore.GetDatasetCalls()), ShouldEqual, 0)
				auditMock.AssertRecordCalls(
					audit_mock.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: genericAuditParams},
				)
			})
		})
	})

	Convey("given audit action successful returns an error", t, func() {
		auditMock := audit_mock.New()
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
			api := GetAPIWithMockedDatastore(mockDatastore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(len(mockDatastore.GetDatasetCalls()), ShouldEqual, 1)
				assertInternalServerErr(w)
				auditMock.AssertRecordCalls(
					audit_mock.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: genericAuditParams},
					audit_mock.Expected{Action: getDatasetAction, Result: audit.Successful, Params: genericAuditParams},
				)
			})
		})
	})

	Convey("given auditing action unsuccessful returns an error", t, func() {
		auditMock := audit_mock.New()
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

			api := GetAPIWithMockedDatastore(mockDatastore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				assertInternalServerErr(w)
				So(len(mockDatastore.GetDatasetCalls()), ShouldEqual, 1)
				auditMock.AssertRecordCalls(
					audit_mock.Expected{Action: getDatasetAction, Result: audit.Attempted, Params: genericAuditParams},
					audit_mock.Expected{Action: getDatasetAction, Result: audit.Unsuccessful, Params: genericAuditParams},
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
		mockedDataStore.UpsertDataset("123", &models.DatasetUpdate{Next: &models.Dataset{}})

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, audit_mock.New(), genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusCreated)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 2)
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
				return errBadRequest
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, audit_mock.New(), genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldResemble, "Failed to parse json body\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		var b string
		b = datasetPayload
		r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return nil, errInternal
			},
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, audit_mock.New(), genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "internal error\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, audit_mock.New(), genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, audit_mock.New(), genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(w.Body.String(), ShouldResemble, "forbidden - dataset already exists\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
	})
}

func TestPostDatasetAuditErrors(t *testing.T) {
	ap := common.Params{"dataset_id": "123"}

	Convey("given audit action attempted returns an error", t, func() {
		auditor := audit_mock.NewErroring(addDatasetAction, audit.Attempted)

		Convey("when add dataset is called", func() {
			r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString("{"))
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {

				assertInternalServerErr(w)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: ap},
				)
			})
		})
	})

	Convey("given audit action unsuccessful returns an error", t, func() {
		auditor := audit_mock.NewErroring(addDatasetAction, audit.Unsuccessful)

		Convey("when datastore getdataset returns an error", func() {
			r, err := createRequestWithAuth("POST", "http://localhost:22000/datasets/123", bytes.NewBufferString("{"))
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return nil, errors.New("get dataset error")
				},
			}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				assertInternalServerErr(w)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: addDatasetAction, Result: audit.Unsuccessful, Params: ap},
				)
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
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 403 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(strings.TrimSpace(w.Body.String()), ShouldEqual, errs.ErrAddDatasetAlreadyExists.Error())

				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: addDatasetAction, Result: audit.Unsuccessful, Params: ap},
				)
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
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				assertInternalServerErr(w)

				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: addDatasetAction, Result: audit.Unsuccessful, Params: ap},
				)
			})
		})
	})

	Convey("given audit action successful returns an error", t, func() {
		auditor := audit_mock.NewErroring(addDatasetAction, audit.Successful)

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
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 201 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusCreated)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: addDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: addDatasetAction, Result: audit.Successful, Params: ap},
				)
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

		dataset := &models.Dataset{
			Title: "CPI",
		}
		mockedDataStore.UpdateDataset("123", dataset, models.CreatedState)

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 2)
		auditor.AssertRecordCalls(
			audit_mock.Expected{Action: putDatasetAction, Result: audit.Attempted, Params: common.Params{"dataset_id": "123"}},
			audit_mock.Expected{Action: putDatasetAction, Result: audit.Successful, Params: common.Params{"dataset_id": "123"}},
		)
	})
}

func TestPutDatasetReturnsError(t *testing.T) {
	t.Parallel()
	ap := common.Params{"dataset_id": "123"}
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		var b string
		b = "{"
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{Next: &models.Dataset{}}, nil
			},
			UpdateDatasetFunc: func(string, *models.Dataset, string) error {
				return errBadRequest
			},
		}

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldResemble, "Failed to parse json body\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
		auditor.AssertRecordCalls(
			audit_mock.Expected{Action: putDatasetAction, Result: audit.Attempted, Params: ap},
			audit_mock.Expected{Action: putDatasetAction, Result: audit.Unsuccessful, Params: ap},
		)
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
				return errInternal
			},
		}

		dataset := &models.Dataset{
			Title: "CPI",
		}
		mockedDataStore.UpdateDataset("123", dataset, models.CreatedState)

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "internal error\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 2)
		auditor.AssertRecordCalls(
			audit_mock.Expected{Action: putDatasetAction, Result: audit.Attempted, Params: ap},
			audit_mock.Expected{Action: putDatasetAction, Result: audit.Unsuccessful, Params: ap},
		)
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

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Dataset not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
		auditor.AssertRecordCalls(
			audit_mock.Expected{Action: putDatasetAction, Result: audit.Attempted, Params: ap},
			audit_mock.Expected{Action: putDatasetAction, Result: audit.Unsuccessful, Params: ap},
		)
	})

	Convey("When the request is not authorised to update dataset return status not found", t, func() {
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

		auditor := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
		So(len(auditor.RecordCalls()), ShouldEqual, 0)
	})
}

func TestPutDatasetAuditErrors(t *testing.T) {
	ap := common.Params{"dataset_id": "123"}

	t.Parallel()
	Convey("given audit action attempted returns an error", t, func() {
		auditor := audit_mock.NewErroring(putDatasetAction, audit.Attempted)

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

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)

			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
				auditor.AssertRecordCalls(audit_mock.Expected{Action: putDatasetAction, Result: audit.Attempted, Params: ap})
			})
		})
	})

	Convey("given audit action successful returns an error", t, func() {
		auditor := audit_mock.NewErroring(putDatasetAction, audit.Successful)

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

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 200 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 1)
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: putDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: putDatasetAction, Result: audit.Successful, Params: ap},
				)
			})
		})
	})

	Convey("given audit action unsuccessful returns an error", t, func() {
		auditor := audit_mock.NewErroring(putDatasetAction, audit.Unsuccessful)

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

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 400 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: putDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: putDatasetAction, Result: audit.Unsuccessful, Params: ap},
				)
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

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 400 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: putDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: putDatasetAction, Result: audit.Unsuccessful, Params: ap},
				)
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

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 400 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: putDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: putDatasetAction, Result: audit.Unsuccessful, Params: ap},
				)
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

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 400 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 1)
				auditor.AssertRecordCalls(
					audit_mock.Expected{Action: putDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: putDatasetAction, Result: audit.Unsuccessful, Params: ap},
				)
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
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		auditorMock := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)
		ap := common.Params{"dataset_id": "123"}
		auditorMock.AssertRecordCalls(
			audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: ap},
			audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Successful, Params: ap},
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
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		auditorMock := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)
		ap := common.Params{"dataset_id": "123"}
		auditorMock.AssertRecordCalls(
			audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: ap},
			audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: ap},
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
			DeleteDatasetFunc: func(string) error {
				return errInternal
			},
		}

		auditorMock := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "internal error\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)
		ap := common.Params{"dataset_id": "123"}
		auditorMock.AssertRecordCalls(
			audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: ap},
			audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: ap},
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
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		auditorMock := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

		ap := common.Params{"dataset_id": "123"}
		auditorMock.AssertRecordCalls(
			audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: ap},
			audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: ap},
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
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		auditorMock := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

		ap := common.Params{"dataset_id": "123"}
		auditorMock.AssertRecordCalls(
			audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: ap},
			audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: ap},
		)
	})

	Convey("When the request is not authorised to delete the dataset return status not found", t, func() {
		var b string
		b = "{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("DELETE", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}
		auditorMock := audit_mock.New()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

		api.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)
		So(len(auditorMock.RecordCalls()), ShouldEqual, 0)
	})
}

func TestDeleteDatasetAuditActionAttemptedError(t *testing.T) {
	t.Parallel()
	Convey("given audit action attempted returns an error", t, func() {
		auditorMock := audit_mock.NewErroring(deleteDatasetAction, audit.Attempted)

		Convey("when delete dataset is called", func() {
			r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)

				ap := common.Params{"dataset_id": "123"}
				auditorMock.AssertRecordCalls(
					audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: ap},
				)
			})
		})
	})
}

func TestDeleteDatasetAuditauditUnsuccessfulError(t *testing.T) {
	Convey("given auditing action unsuccessful returns an errors", t, func() {
		auditorMock := audit_mock.NewErroring(deleteDatasetAction, audit.Unsuccessful)

		Convey("when attempting to delete a dataset that does not exist", func() {

			r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return nil, errs.ErrDatasetNotFound
				},
			}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

			api.Router.ServeHTTP(w, r)

			Convey("then a 204 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNoContent)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)

				ap := common.Params{"dataset_id": "123"}
				auditorMock.AssertRecordCalls(
					audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: ap},
				)
			})
		})

		Convey("when dataStore.Backend.GetDataset returns an error", func() {
			auditorMock = audit_mock.NewErroring(deleteDatasetAction, audit.Unsuccessful)

			r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return nil, errors.New("dataStore.Backend.GetDataset error")
				},
			}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)

				ap := common.Params{"dataset_id": "123"}
				auditorMock.AssertRecordCalls(
					audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: ap},
				)
			})
		})

		Convey("when attempting to delete a published dataset", func() {
			auditorMock = audit_mock.NewErroring(deleteDatasetAction, audit.Unsuccessful)

			r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{Current: &models.Dataset{State: models.PublishedState}}, nil
				},
			}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

			api.Router.ServeHTTP(w, r)

			Convey("then a 403 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)

				ap := common.Params{"dataset_id": "123"}
				auditorMock.AssertRecordCalls(
					audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: ap},
				)
			})
		})

		Convey("when dataStore.Backend.DeleteDataset returns an error", func() {
			auditorMock = audit_mock.NewErroring(deleteDatasetAction, audit.Unsuccessful)

			r, err := createRequestWithAuth("DELETE", "http://localhost:22000/datasets/123", nil)
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			mockedDataStore := &storetest.StorerMock{
				GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
					return &models.DatasetUpdate{Next: &models.Dataset{State: models.CompletedState}}, nil
				},
				DeleteDatasetFunc: func(ID string) error {
					return errors.New("DeleteDatasetFunc error")
				},
			}
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

			api.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)

				ap := common.Params{"dataset_id": "123"}
				auditorMock.AssertRecordCalls(
					audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Unsuccessful, Params: ap},
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
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		auditorMock := audit_mock.NewErroring(deleteDatasetAction, audit.Successful)

		Convey("when delete dataset is called", func() {
			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)
			api.Router.ServeHTTP(w, r)

			Convey("then a 204 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusNoContent)
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)

				ap := common.Params{"dataset_id": "123"}
				auditorMock.AssertRecordCalls(
					audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Attempted, Params: ap},
					audit_mock.Expected{Action: deleteDatasetAction, Result: audit.Successful, Params: ap},
				)
			})
		})
	})
}
