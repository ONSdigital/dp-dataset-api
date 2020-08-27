package dimension_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/api"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/url"
	dprequest "github.com/ONSdigital/dp-net/request"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	urlBuilder  = url.NewBuilder("localhost:20000")
	mu          sync.Mutex
	testContext = context.Background()
)

func createRequestWithToken(method, url string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequest(method, url, body)
	ctx := r.Context()
	ctx = dprequest.SetCaller(ctx, "someone@ons.gov.uk")
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

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDimensionNodeIDCalls()), ShouldEqual, 1)
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

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.UpdateDimensionNodeIDCalls()), ShouldEqual, 1)
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

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDimensionNodeIDCalls()), ShouldEqual, 0)
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: "gobbledygook"}, nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDimensionNodeIDCalls()), ShouldEqual, 0)
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

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDimensionNodeIDCalls()), ShouldEqual, 0)
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

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateDimensionNodeIDCalls()), ShouldEqual, 0)
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

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)
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

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDimensionNodeNotFound.Error())
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)
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

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrResourcePublished.Error())
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 0)
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

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(w.Body.String(), ShouldContainSubstring, "unauthenticated request")
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)
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

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 0)
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: "gobbledygook"}, nil
			},
			AddDimensionToInstanceFunc: func(event *models.CachedDimensionOption) error {
				return nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 0)
	})
}

func TestGetDimensionsReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Get dimensions (and their respective nodes) returns ok", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			GetDimensionsFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return &models.DimensionNodeResults{}, nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsFromInstanceCalls()), ShouldEqual, 1)
	})
}

func TestGetDimensionsReturnsNotFound(t *testing.T) {
	t.Parallel()
	Convey("Get dimensions returns not found", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			GetDimensionsFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return nil, errs.ErrDimensionNodeNotFound
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDimensionNodeNotFound.Error())
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsFromInstanceCalls()), ShouldEqual, 1)
	})
}

func TestGetDimensionsAndOptionsReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return nil, errs.ErrInternalServer
			},
			GetDimensionsFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return &models.DimensionNodeResults{}, nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsFromInstanceCalls()), ShouldEqual, 0)
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: "gobbly gook"}, nil
			},
			GetDimensionsFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return &models.DimensionNodeResults{}, nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDimensionsFromInstanceCalls()), ShouldEqual, 0)
	})
}

func TestGetUniqueDimensionAndOptionsReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Get all unique dimensions returns ok", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			GetUniqueDimensionAndOptionsFunc: func(id, dimension string) (*models.DimensionValues, error) {
				return &models.DimensionValues{}, nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetUniqueDimensionAndOptionsCalls()), ShouldEqual, 1)
	})
}

func TestGetUniqueDimensionAndOptionsReturnsNotFound(t *testing.T) {
	t.Parallel()
	Convey("Get all unique dimensions returns not found", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			GetUniqueDimensionAndOptionsFunc: func(id, dimension string) (*models.DimensionValues, error) {
				return nil, errs.ErrInstanceNotFound
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetUniqueDimensionAndOptionsCalls()), ShouldEqual, 1)
	})
}

func TestGetUniqueDimensionAndOptionsReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return nil, errs.ErrInternalServer
			},
			GetDimensionsFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return &models.DimensionNodeResults{}, nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetUniqueDimensionAndOptionsCalls()), ShouldEqual, 0)
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: "gobbly gook"}, nil
			},
			GetDimensionsFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return &models.DimensionNodeResults{}, nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetUniqueDimensionAndOptionsCalls()), ShouldEqual, 0)
	})
}

func getAPIWithMocks(ctx context.Context, mockedDataStore store.Storer, mockedGeneratedDownloads api.DownloadsGenerator) *api.DatasetAPI {
	mu.Lock()
	defer mu.Unlock()

	cfg, err := config.Get()
	So(err, ShouldBeNil)
	cfg.ServiceAuthToken = "dataset"
	cfg.DatasetAPIURL = "http://localhost:22000"
	cfg.EnablePrivateEndpoints = true

	datasetPermissions := getAuthorisationHandlerMock()
	permissions := getAuthorisationHandlerMock()

	return api.NewDatasetAPI(ctx, *cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedGeneratedDownloads, datasetPermissions, permissions)
}

func getAuthorisationHandlerMock() *mocks.AuthHandlerMock {
	return &mocks.AuthHandlerMock{
		Required: &mocks.PermissionCheckCalls{Calls: 0},
	}
}
