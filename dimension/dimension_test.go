package dimension_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ONSdigital/dp-dataset-api/api"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/dimension"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/url"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/go-ns/log"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	host          = "http://localhost:22000"
	authToken     = "dataset"
	healthTimeout = 2 * time.Second
)

var (
	urlBuilder = url.NewBuilder("localhost:20000")
)

func verifyAuditorCalls(callInfo struct {
	Ctx    context.Context
	Action string
	Result string
	Params common.Params
}, a string, r string, p common.Params) {
	So(callInfo.Action, ShouldEqual, a)
	So(callInfo.Result, ShouldEqual, r)
	So(callInfo.Params, ShouldResemble, p)
}

func createRequestWithToken(method, url string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequest(method, url, body)
	ctx := r.Context()
	ctx = common.SetCaller(ctx, "someone@ons.gov.uk")
	r = r.WithContext(ctx)
	return r, err
}

func TestAddNodeIDToDimensionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("Add node id to a dimension returns ok", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			UpdateDimensionNodeIDFunc: func(event *models.DimensionOption) error {
				return nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDimensionNodeIDCalls()), ShouldEqual, 1)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PutNodeIDAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PutNodeIDAction, audit.Successful, common.Params{"dimension_name": "age", "instance_id": "123", "node_id": "11", "option": "55"})
	})
}

func TestAddNodeIDToDimensionReturnsBadRequest(t *testing.T) {
	t.Parallel()
	Convey("Add node id to a dimension returns bad request", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			UpdateDimensionNodeIDFunc: func(event *models.DimensionOption) error {
				return errs.ErrDimensionNodeNotFound
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDimensionNodeIDCalls()), ShouldEqual, 1)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PutNodeIDAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PutNodeIDAction, audit.Unsuccessful, common.Params{"dimension_name": "age", "instance_id": "123", "node_id": "11", "option": "55"})
	})
}

func TestAddNodeIDToDimensionReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return nil, errs.ErrInternalServer
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDimensionNodeIDCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PutNodeIDAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PutNodeIDAction, audit.Unsuccessful, common.Params{"instance_id": "123"})
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: "gobbly gook"}, nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDimensionNodeIDCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PutNodeIDAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PutNodeIDAction, audit.Unsuccessful, common.Params{"dimension_name": "age", "instance_id": "123", "node_id": "11", "option": "55"})
	})
}

func TestAddNodeIDToDimensionReturnsForbidden(t *testing.T) {
	t.Parallel()
	Convey("Add node id to a dimension of a published instance returns forbidden", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDimensionNodeIDCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PutNodeIDAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PutNodeIDAction, audit.Unsuccessful, common.Params{"instance_id": "123"})
	})
}

func TestAddNodeIDToDimensionReturnsUnauthorized(t *testing.T) {
	t.Parallel()
	Convey("Add node id to a dimension of an instance returns unauthorized", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDimensionNodeIDCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 0)
	})
}

func TestAddNodeIDToDimensionAuditFailure(t *testing.T) {
	t.Parallel()
	Convey("When auditing add node id to dimension attempt fails return an error of internal server error", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return nil, nil
			},
		}

		auditorMock := newAuditorMock()
		auditorMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return errors.New("unable to send message to kafka audit topic")
		}

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, "internal server error")
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 1)

		verifyAuditorCalls(calls[0], dimension.PutNodeIDAction, audit.Attempted, common.Params{"instance_id": "123"})
	})

	Convey("When request to add node id to dimension is forbidden but audit fails returns an error of internal server error", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
		}

		count := 1
		auditorMock := newAuditorMock()
		auditorMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if count == 1 {
				count++
				return nil
			}
			return errors.New("unable to send message to kafka audit topic")
		}

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, "internal server error")
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PutNodeIDAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PutNodeIDAction, audit.Unsuccessful, common.Params{"instance_id": "123"})
	})

	Convey("When request to add node id to dimension and audit fails to send success message return 200 response", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			UpdateDimensionNodeIDFunc: func(event *models.DimensionOption) error {
				return nil
			},
		}

		count := 1
		auditorMock := newAuditorMock()
		auditorMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if count <= 2 {
				count++
				return nil
			}
			return errors.New("unable to send message to kafka audit topic")
		}

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDimensionNodeIDCalls()), ShouldEqual, 1)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PutNodeIDAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PutNodeIDAction, audit.Successful, common.Params{"dimension_name": "age", "instance_id": "123", "node_id": "11", "option": "55"})
	})
}

func TestAddDimensionToInstanceReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Add a dimension to an instance returns ok", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:22000/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			AddDimensionToInstanceFunc: func(event *models.CachedDimensionOption) error {
				return nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PostDimensionsAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PostDimensionsAction, audit.Successful, common.Params{"instance_id": "123"})
	})
}

func TestAddDimensionToInstanceReturnsNotFound(t *testing.T) {
	t.Parallel()
	Convey("Add a dimension to an instance returns not found", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			AddDimensionToInstanceFunc: func(event *models.CachedDimensionOption) error {
				return errs.ErrDimensionNodeNotFound
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PostDimensionsAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PostDimensionsAction, audit.Unsuccessful, common.Params{"instance_id": "123"})
	})
}

func TestAddDimensionToInstanceReturnsForbidden(t *testing.T) {
	t.Parallel()
	Convey("Add a dimension to a published instance returns forbidden", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
			AddDimensionToInstanceFunc: func(event *models.CachedDimensionOption) error {
				return nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PostDimensionsAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PostDimensionsAction, audit.Unsuccessful, common.Params{"instance_id": "123"})
	})
}

func TestAddDimensionToInstanceReturnsUnauthorized(t *testing.T) {
	t.Parallel()
	Convey("Add a dimension to a instance returns unauthorized", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := http.NewRequest("POST", "http://localhost:21800/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 0)
	})
}

func TestAddDimensionToInstanceReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return nil, errs.ErrInternalServer
			},
			AddDimensionToInstanceFunc: func(event *models.CachedDimensionOption) error {
				return nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PostDimensionsAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PostDimensionsAction, audit.Unsuccessful, common.Params{"instance_id": "123"})
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: "gobbly gook"}, nil
			},
			AddDimensionToInstanceFunc: func(event *models.CachedDimensionOption) error {
				return nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PostDimensionsAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PostDimensionsAction, audit.Unsuccessful, common.Params{"instance_id": "123"})
	})
}

func TestAddDimensionAuditFailure(t *testing.T) {
	t.Parallel()
	Convey("When a valid request to add dimension is made but the audit attempt fails returns an error of internal server error", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return nil, nil
			},
		}

		auditorMock := newAuditorMock()
		auditorMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			return errors.New("unable to send message to kafka audit topic")
		}

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, "internal server error")
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 1)

		verifyAuditorCalls(calls[0], dimension.PostDimensionsAction, audit.Attempted, common.Params{"instance_id": "123"})
	})

	Convey("When request to add a dimension is forbidden but audit fails returns an error of internal server error", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
		}

		count := 1
		auditorMock := newAuditorMock()
		auditorMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if count == 1 {
				count++
				return nil
			}
			return errors.New("unable to send message to kafka audit topic")
		}

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, "internal server error")
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PostDimensionsAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PostDimensionsAction, audit.Unsuccessful, common.Params{"instance_id": "123"})
	})

	Convey("When request to add dimension and audit fails to send success message return 200 response", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			AddDimensionToInstanceFunc: func(event *models.CachedDimensionOption) error {
				return nil
			},
		}

		count := 1
		auditorMock := newAuditorMock()
		auditorMock.RecordFunc = func(ctx context.Context, action string, result string, params common.Params) error {
			if count <= 2 {
				count++
				return nil
			}
			return errors.New("unable to send message to kafka audit topic")
		}

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 2)

		verifyAuditorCalls(calls[0], dimension.PostDimensionsAction, audit.Attempted, common.Params{"instance_id": "123"})
		verifyAuditorCalls(calls[1], dimension.PostDimensionsAction, audit.Successful, common.Params{"instance_id": "123"})
	})
}

func TestGetDimensionNodesReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Get dimension nodes returns ok", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			GetDimensionNodesFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return &models.DimensionNodeResults{}, nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionNodesFromInstanceCalls()), ShouldEqual, 1)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 0)
	})
}

func TestGetDimensionNodesReturnsNotFound(t *testing.T) {
	t.Parallel()
	Convey("Get dimension nodes returns not found", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			GetDimensionNodesFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return nil, errs.ErrInstanceNotFound
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionNodesFromInstanceCalls()), ShouldEqual, 1)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 0)
	})
}

func TestGetDimensionNodesReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return nil, errs.ErrInternalServer
			},
			GetDimensionNodesFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return &models.DimensionNodeResults{}, nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionNodesFromInstanceCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 0)
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: "gobbly gook"}, nil
			},
			GetDimensionNodesFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return &models.DimensionNodeResults{}, nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionNodesFromInstanceCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 0)
	})
}

func TestGetUniqueDimensionValuesReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Get all unique dimensions returns ok", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			GetUniqueDimensionValuesFunc: func(id, dimension string) (*models.DimensionValues, error) {
				return &models.DimensionValues{}, nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetUniqueDimensionValuesCalls()), ShouldEqual, 1)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 0)
	})
}

func TestGetUniqueDimensionValuesReturnsNotFound(t *testing.T) {
	t.Parallel()
	Convey("Get all unique dimensions returns not found", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			GetUniqueDimensionValuesFunc: func(id, dimension string) (*models.DimensionValues, error) {
				return nil, errs.ErrInstanceNotFound
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetUniqueDimensionValuesCalls()), ShouldEqual, 1)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 0)
	})
}

func TestGetUniqueDimensionValuesReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return nil, errs.ErrInternalServer
			},
			GetDimensionNodesFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return &models.DimensionNodeResults{}, nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetUniqueDimensionValuesCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 0)
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: "gobbly gook"}, nil
			},
			GetDimensionNodesFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return &models.DimensionNodeResults{}, nil
			},
		}

		auditorMock := newAuditorMock()

		datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditorMock, &mocks.ObservationStoreMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetUniqueDimensionValuesCalls()), ShouldEqual, 0)

		calls := auditorMock.RecordCalls()
		So(len(calls), ShouldEqual, 0)
	})
}

func newAuditorMock() *audit.AuditorServiceMock {
	return &audit.AuditorServiceMock{
		RecordFunc: func(ctx context.Context, action string, result string, params common.Params) error {
			log.Debug("capturing audit event", nil)
			return nil
		},
	}
}

func getAPIWithMockedDatastore(mockedDataStore store.Storer, mockedGeneratedDownloads api.DownloadsGenerator, mockAuditor api.Auditor, mockedObservationStore api.ObservationStore) *api.DatasetAPI {
	cfg, err := config.Get()
	So(err, ShouldBeNil)
	cfg.ServiceAuthToken = authToken
	cfg.DatasetAPIURL = host
	cfg.EnablePrivateEnpoints = true
	cfg.HealthCheckTimeout = healthTimeout

	return api.Routes(*cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedGeneratedDownloads, mockAuditor, mockedObservationStore)
}
