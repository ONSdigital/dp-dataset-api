package api

import (
	"bytes"
	"context"
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
	"github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/ONSdigital/go-ns/audit"
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
	auditParams                   = common.Params{"dataset_id": "123-456"}
)

// GetAPIWithMockedDatastore also used in other tests, so exported
func GetAPIWithMockedDatastore(mockedDataStore store.Storer, mockedGeneratedDownloads DownloadsGenerator, mockAuditor Auditor, mockedObservationStore ObservationStore) *DatasetAPI {
	cfg, err := config.Get()
	So(err, ShouldBeNil)
	cfg.ServiceAuthToken = authToken
	cfg.DatasetAPIURL = host
	cfg.EnablePrivateEnpoints = true
	cfg.HealthCheckTimeout = healthTimeout

	return routes(*cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedGeneratedDownloads, mockAuditor, mockedObservationStore)
}

func createRequestWithAuth(method, URL string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequest(method, URL, body)
	ctx := r.Context()
	ctx = common.SetCaller(ctx, "someone@ons.gov.uk")
	r = r.WithContext(ctx)
	return r, err
}

func verifyAuditRecordCalls(c struct {
	Ctx    context.Context
	Action string
	Result string
	Params common.Params
}, expectedAction string, expectedResult string, expectedParams common.Params) {
	So(c.Action, ShouldEqual, expectedAction)
	So(c.Result, ShouldEqual, expectedResult)
	So(c.Params, ShouldResemble, expectedParams)
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

		mockAuditor := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, mockAuditor, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)

		recCalls := mockAuditor.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getDatasetsAction, actionAttempted, nil)
		verifyAuditRecordCalls(recCalls[1], getDatasetsAction, actionSuccessful, nil)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
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

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return errors.New("boom!")
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 1)
		verifyAuditRecordCalls(recCalls[0], getDatasetsAction, actionAttempted, nil)
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

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getDatasetsAction && result == actionUnsuccessful {
				return errors.New("boom!")
			}
			return nil
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getDatasetsAction, actionAttempted, nil)
		verifyAuditRecordCalls(recCalls[1], getDatasetsAction, actionUnsuccessful, nil)
		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getDatasetsAction, actionAttempted, nil)
		verifyAuditRecordCalls(recCalls[1], getDatasetsAction, actionUnsuccessful, nil)

		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
	})
}

func TestGetDatasetsAuditActionSuccessfulError(t *testing.T) {
	t.Parallel()
	Convey("when a successful request to get dataset fails to audit action successful then a 500 response is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetsFunc: func() ([]models.DatasetUpdate, error) {
				return []models.DatasetUpdate{}, nil
			},
		}

		mockAuditor := getMockAuditor()
		mockAuditor.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getDatasetsAction && result == actionSuccessful {
				return errors.New("boom")
			}
			return nil
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, mockAuditor, genericMockedObservationStore)
		api.router.ServeHTTP(w, r)

		recCalls := mockAuditor.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getDatasetsAction, actionAttempted, nil)
		verifyAuditRecordCalls(recCalls[1], getDatasetsAction, actionSuccessful, nil)

		So(len(mockedDataStore.GetDatasetsCalls()), ShouldEqual, 1)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getDatasetAction, actionAttempted, auditParams)
		verifyAuditRecordCalls(recCalls[1], getDatasetAction, actionSuccessful, auditParams)

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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getDatasetAction, actionAttempted, auditParams)
		verifyAuditRecordCalls(recCalls[1], getDatasetAction, actionSuccessful, auditParams)
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
				return nil, errInternal
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		verifyAuditRecordCalls(recCalls[0], getDatasetAction, actionAttempted, auditParams)
		verifyAuditRecordCalls(recCalls[1], getDatasetAction, actionUnsuccessful, auditParams)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
	})

	Convey("When dataset document has only a next sub document return status 404", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Next: &models.Dataset{ID: "123"}}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
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

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		verifyAuditRecordCalls(recCalls[0], getDatasetAction, actionAttempted, auditParams)
		verifyAuditRecordCalls(recCalls[1], getDatasetAction, actionUnsuccessful, auditParams)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
	})
}

func TestGetDatasetAuditingErrors(t *testing.T) {
	Convey("when auditing get dataset attempt action returns an error then a 500 response is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		w := httptest.NewRecorder()

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return errors.New("auditing error")
		}
		mockDatastore := &storetest.StorerMock{}
		api := GetAPIWithMockedDatastore(mockDatastore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 1)
		verifyAuditRecordCalls(recCalls[0], getDatasetAction, actionAttempted, auditParams)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(mockDatastore.GetDatasetCalls()), ShouldEqual, 0)
	})

	Convey("when auditing get dataset successful action returns an error then a 500 response is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		w := httptest.NewRecorder()

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getDatasetAction && result == actionSuccessful {
				return errors.New("auditing error")
			}
			return nil
		}

		mockDatastore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{ID: "123", Current: &models.Dataset{ID: "123"}}, nil
			},
		}
		api := GetAPIWithMockedDatastore(mockDatastore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getDatasetAction, actionAttempted, auditParams)
		verifyAuditRecordCalls(recCalls[1], getDatasetAction, actionSuccessful, auditParams)

		So(len(mockDatastore.GetDatasetCalls()), ShouldEqual, 1)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
	})

	Convey("when get dataset errors and auditing action unsuccessful errors then a 500 response is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456", nil)
		w := httptest.NewRecorder()

		mockDatastore := &storetest.StorerMock{
			GetDatasetFunc: func(id string) (*models.DatasetUpdate, error) {
				return nil, errors.New("get dataset error")
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getDatasetAction && result == actionUnsuccessful {
				return errors.New("auditing error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockDatastore, &mocks.DownloadsGeneratorMock{}, auditMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getDatasetAction, actionAttempted, auditParams)
		verifyAuditRecordCalls(recCalls[1], getDatasetAction, actionUnsuccessful, auditParams)

		So(len(mockDatastore.GetDatasetCalls()), ShouldEqual, 1)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(w.Body.String(), ShouldResemble, "forbidden - dataset already exists\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 2)
	})
}

func TestPutDatasetReturnsError(t *testing.T) {
	t.Parallel()
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldResemble, "Failed to parse json body\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "internal error\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 2)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Dataset not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor(), genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
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

		auditorMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		calls := auditorMock.RecordCalls()
		ap := common.Params{"dataset_id": "123"}

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], deleteDatasetAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], deleteDatasetAction, actionSuccessful, ap)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
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
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		auditorMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		ap := common.Params{"dataset_id": "123"}
		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], deleteDatasetAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], deleteDatasetAction, actionUnsuccessful, ap)
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

		auditorMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "internal error\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 1)

		calls := auditorMock.RecordCalls()
		ap := common.Params{"dataset_id": "123"}

		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], deleteDatasetAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], deleteDatasetAction, actionUnsuccessful, ap)
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

		auditorMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNoContent)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		ap := common.Params{"dataset_id": "123"}

		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], deleteDatasetAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], deleteDatasetAction, actionUnsuccessful, ap)
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

		auditorMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		ap := common.Params{"dataset_id": "123"}
		So(len(calls), ShouldEqual, 2)
		verifyAuditRecordCalls(calls[0], deleteDatasetAction, actionAttempted, ap)
		verifyAuditRecordCalls(calls[1], deleteDatasetAction, actionUnsuccessful, ap)
	})

	Convey("When the request is not authorised to delete the dataset return status not found", t, func() {
		var b string
		b = "{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("DELETE", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}
		auditorMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, genericMockedObservationStore)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)
		So(len(auditorMock.RecordCalls()), ShouldEqual, 0)
	})
}

func getMockAuditor() *audit.AuditorServiceMock {
	return &audit.AuditorServiceMock{
		RecordFunc: func(ctx context.Context, action string, result string, params common.Params) error {
			return nil
		},
	}
}
