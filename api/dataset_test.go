package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"gopkg.in/mgo.v2/bson"

	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/go-ns/log"

	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/gorilla/mux"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
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

	datasetPayload           = `{"contacts":[{"email":"testing@hotmail.com","name":"John Cox","telephone":"01623 456789"}],"description":"census","links":{"access_rights":{"href":"http://ons.gov.uk/accessrights"}},"title":"CensusEthnicity","theme":"population","periodicity":"yearly","state":"completed","next_release":"2016-04-04","publisher":{"name":"The office of national statistics","type":"government department","url":"https://www.ons.gov.uk/"}}`
	editionPayload           = `{"edition":"2017","state":"created"}`
	versionPayload           = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04"}`
	versionAssociatedPayload = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"associated","collection_id":"12345"}`
	versionPublishedPayload  = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"published","collection_id":"12345"}`

	urlBuilder = url.NewBuilder("localhost:20000")

	auditParams = common.Params{"dataset_id": "123-456"}
)

// GetAPIWithMockedDatastore also used in other tests, so exported
func GetAPIWithMockedDatastore(mockedDataStore store.Storer, mockedGeneratedDownloads DownloadsGenerator, mockAuditor Auditor) *DatasetAPI {
	cfg, err := config.Get()
	So(err, ShouldBeNil)
	cfg.ServiceAuthToken = authToken
	cfg.DatasetAPIURL = host
	cfg.EnablePrivateEnpoints = true
	cfg.HealthCheckTimeout = healthTimeout
	return routes(*cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedGeneratedDownloads, mockAuditor)
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
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, mockAuditor)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

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
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, mockAuditor)
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
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

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
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)
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
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
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
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

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
		api := GetAPIWithMockedDatastore(mockDatastore, &mocks.DownloadsGeneratorMock{}, auditMock)

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
		api := GetAPIWithMockedDatastore(mockDatastore, &mocks.DownloadsGeneratorMock{}, auditMock)

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
		api := GetAPIWithMockedDatastore(mockDatastore, &mocks.DownloadsGeneratorMock{}, auditMock)

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

func TestGetEditionsReturnsOK(t *testing.T) {

	t.Parallel()
	Convey("A successful request to get edition returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionsFunc: func(id string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionsAction, actionAttempted, auditParams)
		verifyAuditRecordCalls(recCalls[1], getEditionsAction, actionSuccessful, auditParams)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionsAuditingError(t *testing.T) {
	t.Parallel()
	Convey("given auditing get editions attempted action returns an error then a 500 response is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionsFunc: func(id string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return errors.New("get editions action attempted audit event error")
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 1)
		verifyAuditRecordCalls(recCalls[0], getEditionsAction, actionAttempted, auditParams)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
	})

	Convey("given auditing get editions action successful returns an error then a 500 response is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionsFunc: func(id string, state string) (*models.EditionUpdateResults, error) {
				return &models.EditionUpdateResults{}, nil
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getEditionsAction && result == actionSuccessful {
				return errors.New("audit error")
			}
			return nil
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionsAction, actionAttempted, auditParams)
		verifyAuditRecordCalls(recCalls[1], getEditionsAction, actionSuccessful, auditParams)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})

	Convey("When the dataset does not exist and auditing the action result fails then return status 500", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrDatasetNotFound
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getEditionsAction && result == actionUnsuccessful {
				return errors.New(auditError)
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionsAction, actionAttempted, auditParams)
		verifyAuditRecordCalls(recCalls[1], getEditionsAction, actionUnsuccessful, auditParams)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
	})

	Convey("When no published editions exist against a published dataset and auditing unsuccessful errors return status 500", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionsFunc: func(id string, state string) (*models.EditionUpdateResults, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getEditionsAction && result == actionUnsuccessful {
				return errors.New(auditError)
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionsAction, actionAttempted, auditParams)
		verifyAuditRecordCalls(recCalls[1], getEditionsAction, actionUnsuccessful, auditParams)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionsReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errInternal
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "internal error\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionsAction, actionAttempted, auditParams)
		verifyAuditRecordCalls(recCalls[1], getEditionsAction, actionUnsuccessful, auditParams)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrDatasetNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Dataset not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionsAction, actionAttempted, auditParams)
		verifyAuditRecordCalls(recCalls[1], getEditionsAction, actionUnsuccessful, auditParams)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 0)
	})

	Convey("When no editions exist against an existing dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionsFunc: func(id string, state string) (*models.EditionUpdateResults, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Edition not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionsAction, actionAttempted, auditParams)
		verifyAuditRecordCalls(recCalls[1], getEditionsAction, actionUnsuccessful, auditParams)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})

	Convey("When no published editions exist against a published dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionsFunc: func(id string, state string) (*models.EditionUpdateResults, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Edition not found\n")

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionsCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("A successful request to get edition returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionFunc: func(id string, editionID string, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{}, nil
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		p := common.Params{"dataset_id": "123-456", "edition": "678"}

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getEditionAction, actionSuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errInternal
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		p := common.Params{"dataset_id": "123-456", "edition": "678"}

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "internal error\n")

		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getEditionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrDatasetNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		p := common.Params{"dataset_id": "123-456", "edition": "678"}

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Dataset not found\n")

		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getEditionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
	})

	Convey("When edition does not exist for a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		r.Header.Add("internal-token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionFunc: func(id string, editionID string, state string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		p := common.Params{"dataset_id": "123-456", "edition": "678"}

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Edition not found\n")

		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getEditionAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})

	Convey("When edition is not published for a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionFunc: func(id string, editionID string, state string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		p := common.Params{"dataset_id": "123-456", "edition": "678"}

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Edition not found\n")
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getEditionAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})
}

func TestGetEditionAuditErrors(t *testing.T) {
	t.Parallel()
	Convey("when auditing get edition attempted action errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return errors.New("auditing error")
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		p := common.Params{"dataset_id": "123-456", "edition": "678"}

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 1)
		verifyAuditRecordCalls(recCalls[0], getEditionAction, actionAttempted, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
	})

	Convey("when check dataset exists errors and auditing action unsuccessful errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID string, state string) error {
				return errors.New("check dataset error")
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getEditionAction && result == actionUnsuccessful {
				return errors.New("auditing error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		p := common.Params{"dataset_id": "123-456", "edition": "678"}

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getEditionAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 0)
	})

	Convey("when check edition exists errors and auditing action unsuccessful errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID string, state string) error {
				return nil
			},
			GetEditionFunc: func(ID, editionID, state string) (*models.EditionUpdate, error) {
				return nil, errors.New("get edition error")
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getEditionAction && result == actionUnsuccessful {
				return errors.New("auditing error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		p := common.Params{"dataset_id": "123-456", "edition": "678"}

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getEditionAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})

	Convey("when get edition audit even successful errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			GetEditionFunc: func(id string, editionID string, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{}, nil
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getEditionAction && result == actionSuccessful {
				return errors.New("error")
			}
			return nil
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()
		p := common.Params{"dataset_id": "123-456", "edition": "678"}

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getEditionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getEditionAction, actionSuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionsReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("A successful request to get version returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID, editionID, state string) (*models.VersionResults, error) {
				return &models.VersionResults{}, nil
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		p := common.Params{"dataset_id": "123-456", "edition": "678"}
		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionSuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionsReturnsError(t *testing.T) {
	t.Parallel()
	p := common.Params{"dataset_id": "123-456", "edition": "678"}

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errInternal
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "internal error\n")
		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrDatasetNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Dataset not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("When the edition of a dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Edition not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("When version does not exist for an edition of a dataset returns status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID, editionID, state string) (*models.VersionResults, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Version not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})

	Convey("When version is not published against an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID, editionID, state string) (*models.VersionResults, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Version not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})

	Convey("When a published version has an incorrect state for an edition of a dataset return an internal error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()

		version := models.Version{State: "gobbly-gook"}
		items := []models.Version{version}
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID, editionID, state string) (*models.VersionResults, error) {
				return &models.VersionResults{Items: items}, nil
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "Incorrect resource state\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionsAuditError(t *testing.T) {
	t.Parallel()
	p := common.Params{"dataset_id": "123-456", "edition": "678"}
	err := errors.New("error")

	Convey("when auditing get versions attempted action errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return err
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 1)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("when auditing check dataset exists error returns an error then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID string, state string) error {
				return err
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("when auditing check edition exists error returns an error then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID string, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
				return err
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 0)
	})

	Convey("when auditing get versions error returns an error then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID string, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID string, editionID string, state string) (*models.VersionResults, error) {
				return nil, err
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})

	Convey("when auditing invalid state returns an error then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(ID string, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID string, editionID string, state string) (*models.VersionResults, error) {
				return &models.VersionResults{
					Items: []models.Version{{State: "not valid"}},
				}, nil
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionUnsuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})

	Convey("when auditing get versions successful event errors then an 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionsFunc: func(datasetID, editionID, state string) (*models.VersionResults, error) {
				return &models.VersionResults{}, nil
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionsAction && result == actionSuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		p := common.Params{"dataset_id": "123-456", "edition": "678"}
		recCalls := auditMock.RecordCalls()

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionsAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionsAction, actionSuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionsCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("A successful request to get version returns 200 OK response", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{
					State: models.EditionConfirmedState,
					Links: &models.VersionLinks{
						Self: &models.LinkObject{},
						Version: &models.LinkObject{
							HRef: "href",
						},
					},
				}, nil
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		p := common.Params{"dataset_id": "123-456", "edition": "678", "version": "1"}
		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionSuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionReturnsError(t *testing.T) {
	p := common.Params{"dataset_id": "123-456", "edition": "678", "version": "1"}
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errInternal
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "internal error\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
	})

	Convey("When the dataset does not exist for return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrDatasetNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Dataset not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the edition of a dataset does not exist return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Edition not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When version does not exist for an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Version not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When version is not published for an edition of a dataset return status not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldResemble, "Version not found\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When an unpublished version has an incorrect state for an edition of a dataset return an internal error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{
					State: "gobbly-gook",
					Links: &models.VersionLinks{
						Self: &models.LinkObject{},
						Version: &models.LinkObject{
							HRef: "href",
						},
					},
				}, nil
			},
		}

		auditMock := getMockAuditor()
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "Incorrect resource state\n")

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestGetVersionAuditErrors(t *testing.T) {
	p := common.Params{"dataset_id": "123-456", "edition": "678", "version": "1"}
	t.Parallel()
	Convey("When auditing get version action attempted errors then a 500 status is returned", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errInternal
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionAction && result == actionAttempted {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 1)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset does not exist and audit errors then return a 500 status", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return errs.ErrDatasetNotFound
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the edition does not exist for a dataset and auditing errors then a 500 status", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return errs.ErrEditionNotFound
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When version does not exist for an edition of a dataset and auditing errors then a 500 status", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When version does not exist for an edition of a dataset and auditing errors then a 500 status", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)
		r.Header.Add("internal_token", "coffee")
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{
					State: "indifferent",
					Links: &models.VersionLinks{
						Self:    &models.LinkObject{HRef: "self"},
						Version: &models.LinkObject{HRef: "version"},
					},
				}, nil
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionAction && result == actionUnsuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)

		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionUnsuccessful, p)

		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("when auditing a successful request to get a version errors then return a 500 status", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123-456/editions/678/versions/1", nil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			CheckDatasetExistsFunc: func(datasetID, state string) error {
				return nil
			},
			CheckEditionExistsFunc: func(datasetID, editionID, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, editionID, version, state string) (*models.Version, error) {
				return &models.Version{
					State: models.EditionConfirmedState,
					Links: &models.VersionLinks{
						Self: &models.LinkObject{},
						Version: &models.LinkObject{
							HRef: "href",
						},
					},
				}, nil
			},
		}

		auditMock := getMockAuditor()
		auditMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if action == getVersionAction && result == actionSuccessful {
				return errors.New("error")
			}
			return nil
		}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditMock)

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, internalServerErr)
		recCalls := auditMock.RecordCalls()
		So(len(recCalls), ShouldEqual, 2)
		verifyAuditRecordCalls(recCalls[0], getVersionAction, actionAttempted, p)
		verifyAuditRecordCalls(recCalls[1], getVersionAction, actionSuccessful, p)
		So(len(mockedDataStore.CheckDatasetExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNoContent)
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

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
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
			DeleteDatasetFunc: func(string) error {
				return errInternal
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldResemble, "internal error\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
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
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNoContent)

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
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
			DeleteDatasetFunc: func(string) error {
				return nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the request is not authorised to delete the dataset return status not found", t, func() {
		var b string
		b = "{\"edition\":\"2017\",\"state\":\"created\",\"license\":\"ONS\",\"release_date\":\"2017-04-04\",\"version\":\"1\"}"
		r, err := http.NewRequest("DELETE", "http://localhost:22000/datasets/123", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}
		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())

		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(w.Body.String(), ShouldResemble, "unauthenticated request\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.DeleteDatasetCalls()), ShouldEqual, 0)
	})
}

func TestPutVersionReturnsSuccessfully(t *testing.T) {
	t.Parallel()
	Convey("When state is unchanged", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					ID: "789",
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
							ID:   "123",
						},
						Dimensions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017",
							ID:   "456",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
						},
					},
					ReleaseDate: "2017-12-12",
					State:       models.EditionConfirmedState,
				}, nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
		}
		mockedDataStore.GetVersion("123", "2017", "1", "")
		mockedDataStore.UpdateVersion("a1b2c3", &models.Version{})

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 3)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When state is set to associated", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionAssociatedPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: models.AssociatedState,
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
		mockedDataStore.UpdateDatasetWithAssociation("123", models.AssociatedState, &models.Version{})

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 3)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When state is set to edition-confirmed", t, func() {
		downloadsGenerated := make(chan bool, 1)

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				downloadsGenerated <- true
				return nil
			},
		}

		var b string
		b = versionAssociatedPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: models.EditionConfirmedState,
				}, nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
			UpdateDatasetWithAssociationFunc: func(string, string, *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, getMockAuditor())
		api.router.ServeHTTP(w, r)

		select {
		case <-downloadsGenerated:
			log.Info("download generated as expected", nil)
		case <-time.After(time.Second * 10):
			err := errors.New("failing test due to timeout")
			log.Error(err, nil)
			t.Fail()
		}

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 1)
	})

	Convey("When state is set to published", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPublishedPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					ID: "789",
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
							ID:   "123",
						},
						Dimensions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017",
							ID:   "2017",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/instances/765",
						},
						Version: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
						},
					},
					ReleaseDate: "2017-12-12",
					Downloads: &models.DownloadList{
						CSV: &models.DownloadObject{
							Private: "s3://csv-exported/myfile.csv",
							HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.csv",
							Size:    "1234",
						},
					},
					State: models.EditionConfirmedState,
				}, nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
			GetDatasetFunc: func(string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID:      "123",
					Next:    &models.Dataset{Links: &models.DatasetLinks{}},
					Current: &models.Dataset{Links: &models.DatasetLinks{}},
				}, nil
			},
			UpsertDatasetFunc: func(string, *models.DatasetUpdate) error {
				return nil
			},
			GetEditionFunc: func(string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "123",
					Next: &models.Edition{
						State: models.PublishedState,
					},
					Current: &models.Edition{},
				}, nil
			},
			UpsertEditionFunc: func(string, string, *models.EditionUpdate) error {
				return nil
			},
		}

		mockedDataStore.GetVersion("789", "2017", "1", "")
		mockedDataStore.GetEdition("123", "2017", "")
		mockedDataStore.UpdateVersion("a1b2c3", &models.Version{})
		mockedDataStore.GetDataset("123")
		mockedDataStore.UpsertDataset("123", &models.DatasetUpdate{Next: &models.Dataset{}})

		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 3)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 1)
	})
}

func TestPutVersionGenerateDownloadsError(t *testing.T) {
	Convey("given download generator returns an error", t, func() {

		mockedErr := errors.New("spectacular explosion")
		var v models.Version
		json.Unmarshal([]byte(versionAssociatedPayload), &v)
		v.State = models.EditionConfirmedState

		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
				return &v, nil
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
				return nil
			},
			UpdateVersionFunc: func(ID string, version *models.Version) error {
				return nil
			},
			UpdateDatasetWithAssociationFunc: func(ID string, state string, version *models.Version) error {
				return nil
			},
		}

		mockDownloadGenerator := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return mockedErr
			},
		}

		Convey("when put version is called with a valid request", func() {
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionAssociatedPayload))
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()
			cfg, err := config.Get()
			So(err, ShouldBeNil)
			cfg.EnablePrivateEnpoints = true
			api := routes(*cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockDownloadGenerator, nil)
			api.router.ServeHTTP(w, r)

			Convey("then an internal server error response is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
			})

			Convey("and the expected store calls are made with the expected parameters", func() {
				genCalls := mockDownloadGenerator.GenerateCalls()

				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(mockedDataStore.GetDatasetCalls()[0].ID, ShouldEqual, "123")

				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(mockedDataStore.CheckEditionExistsCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.CheckEditionExistsCalls()[0].EditionID, ShouldEqual, "2017")

				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
				So(mockedDataStore.GetVersionCalls()[0].DatasetID, ShouldEqual, "123")
				So(mockedDataStore.GetVersionCalls()[0].EditionID, ShouldEqual, "2017")
				So(mockedDataStore.GetVersionCalls()[0].Version, ShouldEqual, "1")

				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)

				So(len(genCalls), ShouldEqual, 1)
				So(genCalls[0].DatasetID, ShouldEqual, "123")
				So(genCalls[0].Edition, ShouldEqual, "2017")
				So(genCalls[0].Version, ShouldEqual, "1")
			})
		})
	})
}

func TestPutEmptyVersion(t *testing.T) {
	var v models.Version
	json.Unmarshal([]byte(versionAssociatedPayload), &v)
	v.State = models.AssociatedState
	xlsDownload := &models.DownloadList{XLS: &models.DownloadObject{Size: "1", HRef: "/hello"}}

	Convey("given an existing version with empty downloads", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
				return &v, nil
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
				return nil
			},
			UpdateVersionFunc: func(ID string, version *models.Version) error {
				return nil
			},
		}

		Convey("when put version is called with an associated version with empty downloads", func() {
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionAssociatedPayload))
			So(err, ShouldBeNil)

			w := httptest.NewRecorder()

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
			api.router.ServeHTTP(w, r)

			Convey("then a http status ok is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
			})

			Convey("and the updated version is as expected", func() {
				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
				So(mockedDataStore.UpdateVersionCalls()[0].Version.Downloads, ShouldBeNil)
			})
		})
	})

	Convey("given an existing version with a xls download already exists", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID string, editionID string, version string, state string) (*models.Version, error) {
				v.Downloads = xlsDownload
				return &v, nil
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(ID string, editionID string, state string) error {
				return nil
			},
			UpdateVersionFunc: func(ID string, version *models.Version) error {
				return nil
			},
		}

		mockDownloadGenerator := &mocks.DownloadsGeneratorMock{}

		Convey("when put version is called with an associated version with empty downloads", func() {
			r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(versionAssociatedPayload))
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
			api.router.ServeHTTP(w, r)

			Convey("then a http status ok is returned", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
			})

			Convey("and any existing version downloads are not overwritten", func() {
				So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
				So(mockedDataStore.UpdateVersionCalls()[0].Version.Downloads, ShouldResemble, xlsDownload)
			})

			Convey("and the expected external calls are made with the correct parameters", func() {
				So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
				So(mockedDataStore.GetDatasetCalls()[0].ID, ShouldEqual, "123")

				So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
				So(mockedDataStore.CheckEditionExistsCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.CheckEditionExistsCalls()[0].EditionID, ShouldEqual, "2017")
				So(mockedDataStore.CheckEditionExistsCalls()[0].State, ShouldEqual, "")

				So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
				So(mockedDataStore.GetVersionCalls()[0].DatasetID, ShouldEqual, "123")
				So(mockedDataStore.GetVersionCalls()[0].EditionID, ShouldEqual, "2017")
				So(mockedDataStore.GetVersionCalls()[0].Version, ShouldEqual, "1")
				So(mockedDataStore.GetVersionCalls()[0].State, ShouldEqual, "")

				So(len(mockedDataStore.UpdateEditionCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
				So(len(mockDownloadGenerator.GenerateCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestPutVersionReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the request contain malformed json a bad request status is returned", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = "{"
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldEqual, "Failed to parse json body\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When the api cannot connect to datastore return an internal server error", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return nil, errInternal
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, "internal error\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset document cannot be found for version return status not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(datasetID string, edition string, versionID string, version string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{}, errs.ErrVersionNotFound
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Dataset not found\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When the edition document cannot be found for version return status not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{}, errs.ErrVersionNotFound
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return errs.ErrEditionNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Edition not found\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When the version document cannot be found return status not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{}, errs.ErrVersionNotFound
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Version not found\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When the request is not authorised to update version then response returns status not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := http.NewRequest("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: "associated",
				}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(w.Body.String(), ShouldEqual, "unauthenticated request\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})

	Convey("When the version document has already been published return status forbidden", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = versionPayload
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{
					State: models.PublishedState,
				}, nil
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(w.Body.String(), ShouldEqual, "unable to update version as it has been published\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 0)
	})

	Convey("When the request body is invalid return status bad request", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(string, string, string, string) error {
				return nil
			},
		}

		var b string
		b = `{"instance_id":"a1b2c3","edition":"2017","license":"ONS","release_date":"2017-04-04","state":"associated"}`
		r, err := createRequestWithAuth("PUT", "http://localhost:22000/datasets/123/editions/2017/versions/1", bytes.NewBufferString(b))
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(string, string, string, string) (*models.Version, error) {
				return &models.Version{State: "associated"}, nil
			},
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{}, nil
			},
			CheckEditionExistsFunc: func(string, string, string) error {
				return nil
			},
			UpdateVersionFunc: func(string, *models.Version) error {
				return nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, generatorMock, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(w.Body.String(), ShouldEqual, "Missing collection_id for association between version and a collection\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
		So(len(generatorMock.GenerateCalls()), ShouldEqual, 0)
	})
}

func TestGetDimensionsReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("When the request contain valid ids return dimension information", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionsFunc: func(datasetID, versionID string) ([]bson.M, error) {
				return []bson.M{}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 1)
	})
}

func TestGetDimensionsReturnsErrors(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore to get dimension resource return an internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return nil, errInternal
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, "internal error\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)
	})

	Convey("When the request contain an invalid version return not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Version not found\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)
	})

	Convey("When there are no dimensions then return not found error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionsFunc: func(datasetID, versionID string) ([]bson.M, error) {
				return nil, errs.ErrDimensionsNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Dimensions not found\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 1)
	})

	Convey("When the version has an invalid state return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: "gobbly-gook"}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, "Incorrect resource state\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsCalls()), ShouldEqual, 0)
	})
}

func TestGetDimensionOptionsReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("When a valid dimension is provided then a list of options can be returned successfully", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionOptionsFunc: func(version *models.Version, dimensions string) (*models.DimensionOptionResults, error) {
				return &models.DimensionOptionResults{}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
	})
}

func TestGetDimensionOptionsReturnsErrors(t *testing.T) {
	t.Parallel()
	Convey("When the version doesn't exist in a request for dimension options, then return not found", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Version not found\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionOptionsCalls()), ShouldEqual, 0)
	})

	Convey("When an internal error causes failure to retrieve dimension options, then return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: models.AssociatedState}, nil
			},
			GetDimensionOptionsFunc: func(version *models.Version, dimensions string) (*models.DimensionOptionResults, error) {
				return nil, errInternal
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, "internal error\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionOptionsCalls()), ShouldEqual, 1)
	})

	Convey("When the version has an invalid state return internal server error", t, func() {
		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions/age/options", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: "gobbly-gook"}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, "Incorrect resource state\n")

		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionOptionsCalls()), ShouldEqual, 0)
	})
}

func TestGetMetadataReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Successfully return metadata resource for a request without an authentication header", t, func() {

		datasetDoc := createDatasetDoc()
		versionDoc := createVersionDoc()

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(datasetID, edition, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return versionDoc, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)

		bytes, err := ioutil.ReadAll(w.Body)
		if err != nil {
			os.Exit(1)
		}

		var metaData models.Metadata

		err = json.Unmarshal(bytes, &metaData)
		if err != nil {
			os.Exit(1)
		}

		So(metaData.Keywords, ShouldBeNil)
		So(metaData.ReleaseFrequency, ShouldEqual, "yearly")

		temporal := models.TemporalFrequency{
			EndDate:   "2017-05-09",
			Frequency: "Monthly",
			StartDate: "2014-05-09",
		}
		So(metaData.Temporal, ShouldResemble, &[]models.TemporalFrequency{temporal})
		So(metaData.UnitOfMeasure, ShouldEqual, "Pounds Sterling")
	})

	// Subtle difference between the test above and below, keywords is Not nil
	// in response for test below while it is nil in test above
	Convey("Successfully return metadata resource for a request with an authentication header", t, func() {

		datasetDoc := createDatasetDoc()
		versionDoc := createVersionDoc()

		r, err := createRequestWithAuth("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(datasetID, edition, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return versionDoc, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)

		bytes, err := ioutil.ReadAll(w.Body)
		if err != nil {
			os.Exit(1)
		}

		var metaData models.Metadata

		err = json.Unmarshal(bytes, &metaData)
		if err != nil {
			os.Exit(1)
		}

		So(metaData.Keywords, ShouldResemble, []string{"pensioners"})
		So(metaData.ReleaseFrequency, ShouldResemble, "yearly")

		temporal := models.TemporalFrequency{
			EndDate:   "2017-05-09",
			Frequency: "Monthly",
			StartDate: "2014-05-09",
		}
		So(metaData.Temporal, ShouldResemble, &[]models.TemporalFrequency{temporal})
		So(metaData.UnitOfMeasure, ShouldEqual, "Pounds Sterling")
	})
}

func TestGetMetadataReturnsError(t *testing.T) {
	t.Parallel()
	Convey("When the api cannot connect to datastore return an internal server error", t, func() {

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return nil, errInternal
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, "internal error\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset document cannot be found for version return status not found", t, func() {

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Dataset not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the dataset document has no current sub document return status not found", t, func() {

		datasetDoc := createDatasetDoc()
		datasetDoc.Current = nil

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(datasetId, edition, state string) error {
				return nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Dataset not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 0)
	})

	Convey("When the edition document cannot be found for version return status not found", t, func() {

		datasetDoc := createDatasetDoc()

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(datasetId, edition, state string) error {
				return errs.ErrEditionNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Edition not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 0)
	})

	Convey("When the version document cannot be found return status not found", t, func() {

		datasetDoc := createDatasetDoc()

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(datasetId, edition, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldEqual, "Version not found\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When the version document state is invalid return an internal server error", t, func() {

		datasetDoc := createDatasetDoc()

		r := httptest.NewRequest("GET", "http://localhost:22000/datasets/123/editions/2017/versions/1/metadata", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(datasetID string) (*models.DatasetUpdate, error) {
				return datasetDoc, nil
			},
			CheckEditionExistsFunc: func(datasetId, edition, state string) error {
				return nil
			},
			GetVersionFunc: func(datasetID, edition, version, state string) (*models.Version, error) {
				return &models.Version{State: "gobbly-gook"}, nil
			},
		}

		api := GetAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, getMockAuditor())
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldEqual, "Incorrect resource state\n")

		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestCreateNewVersionDoc(t *testing.T) {
	t.Parallel()
	Convey("Check the version has the new collection id when request contains a collection_id", t, func() {
		currentVersion := &models.Version{}
		version := &models.Version{
			CollectionID: "4321",
		}

		populateNewVersionDoc(currentVersion, version)
		So(version.CollectionID, ShouldNotBeNil)
		So(version.CollectionID, ShouldEqual, "4321")
	})

	Convey("Check the version collection id does not get replaced by the current collection id when request contains a collection_id", t, func() {
		currentVersion := &models.Version{
			CollectionID: "1234",
		}
		version := &models.Version{
			CollectionID: "4321",
		}

		populateNewVersionDoc(currentVersion, version)
		So(version.CollectionID, ShouldNotBeNil)
		So(version.CollectionID, ShouldEqual, "4321")
	})

	Convey("Check the version has the old collection id when request is missing a collection_id", t, func() {
		currentVersion := &models.Version{
			CollectionID: "1234",
		}
		version := &models.Version{}

		populateNewVersionDoc(currentVersion, version)
		So(version.CollectionID, ShouldNotBeNil)
		So(version.CollectionID, ShouldEqual, "1234")
	})

	Convey("check the version collection id is not set when both request body and current version document are missing a collection id", t, func() {
		currentVersion := &models.Version{}
		version := &models.Version{}

		populateNewVersionDoc(currentVersion, version)
		So(version.CollectionID, ShouldNotBeNil)
		So(version.CollectionID, ShouldEqual, "")
	})

	Convey("Check the version has the new spatial link when request contains a links.spatial.href", t, func() {
		currentVersion := &models.Version{}
		version := &models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/geographylist",
				},
			},
		}

		populateNewVersionDoc(currentVersion, version)
		So(version.Links, ShouldNotBeNil)
		So(version.Links.Spatial, ShouldNotBeNil)
		So(version.Links.Spatial.HRef, ShouldEqual, "http://ons.gov.uk/geographylist")
	})

	Convey("Check the version links.spatial.href does not get replaced by the current version value", t, func() {
		currentVersion := &models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/oldgeographylist",
				},
			},
		}
		version := &models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/geographylist",
				},
			},
		}

		populateNewVersionDoc(currentVersion, version)
		So(version.Links, ShouldNotBeNil)
		So(version.Links.Spatial, ShouldNotBeNil)
		So(version.Links.Spatial.HRef, ShouldEqual, "http://ons.gov.uk/geographylist")
	})

	Convey("Check the links.spatial.href has the old value when request does not contain a links.spatial.href", t, func() {
		currentVersion := &models.Version{
			Links: &models.VersionLinks{
				Spatial: &models.LinkObject{
					HRef: "http://ons.gov.uk/oldgeographylist",
				},
			},
		}
		version := &models.Version{}

		populateNewVersionDoc(currentVersion, version)
		So(version.Links, ShouldNotBeNil)
		So(version.Links.Spatial, ShouldNotBeNil)
		So(version.Links.Spatial.HRef, ShouldEqual, "http://ons.gov.uk/oldgeographylist")
	})

	Convey("check the version links.spatial.href is not set when both request body and current version document do not contain a links.spatial.href", t, func() {
		currentVersion := &models.Version{
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://ons.gov.uk/datasets/123",
				},
			},
		}
		version := &models.Version{}

		populateNewVersionDoc(currentVersion, version)
		So(version.Links, ShouldNotBeNil)
		So(version.Links.Spatial, ShouldBeNil)
	})
}

// createDatasetDoc returns a datasetUpdate doc containing minimal fields but
// there is a clear difference between the current and next sub documents
func createDatasetDoc() *models.DatasetUpdate {
	generalDataset := &models.Dataset{
		CollectionID:     "4321",
		ReleaseFrequency: "yearly",
		State:            models.PublishedState,
		UnitOfMeasure:    "Pounds Sterling",
	}

	nextDataset := *generalDataset
	nextDataset.CollectionID = "3434"
	nextDataset.Keywords = []string{"pensioners"}
	nextDataset.State = models.AssociatedState

	datasetDoc := &models.DatasetUpdate{
		ID:      "123",
		Current: generalDataset,
		Next:    &nextDataset,
	}

	return datasetDoc
}

func createVersionDoc() *models.Version {
	temporal := models.TemporalFrequency{
		EndDate:   "2017-05-09",
		Frequency: "Monthly",
		StartDate: "2014-05-09",
	}
	versionDoc := &models.Version{
		State:        models.EditionConfirmedState,
		CollectionID: "3434",
		Temporal:     &[]models.TemporalFrequency{temporal},
	}

	return versionDoc
}

func getMockAuditor() *audit.AuditorServiceMock {
	return &audit.AuditorServiceMock{
		RecordFunc: func(ctx context.Context, action string, result string, params common.Params) error {
			return nil
		},
	}
}
