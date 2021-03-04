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

func validateDimensionUpdate(mockedDataStore *storetest.StorerMock, expected *models.DimensionOption) {
	// Gets called twice as there is a check wrapper around this route which
	// checks the instance is not published before entering handler
	So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 2)
	So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, expected.InstanceID)
	So(mockedDataStore.GetInstanceCalls()[1].ID, ShouldEqual, expected.InstanceID)

	So(mockedDataStore.UpdateDimensionNodeIDAndOrderCalls(), ShouldHaveLength, 1)
	So(mockedDataStore.UpdateDimensionNodeIDAndOrderCalls()[0].Dimension, ShouldResemble, expected)
}

// Deprecated
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
			UpdateDimensionNodeIDAndOrderFunc: func(event *models.DimensionOption) error {
				return nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		Convey("And the expected database calls are performed to update nodeID and order", func() {
			validateDimensionUpdate(mockedDataStore, &models.DimensionOption{
				InstanceID: "123",
				Name:       "age",
				NodeID:     "11",
				Option:     "55",
				Order:      nil,
			})
		})
	})
}

func TestPatchOptionReturnsOK(t *testing.T) {
	t.Parallel()

	Convey("Given a Dataset API instance with a mocked data store", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			UpdateDimensionNodeIDAndOrderFunc: func(event *models.DimensionOption) error {
				return nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		Convey("Then patch dimension option with a valid node_id returns ok", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/node_id", "value": "11"}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)

			Convey("And the expected database calls are performed to update node_id", func() {
				validateDimensionUpdate(mockedDataStore, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					NodeID:     "11",
					Option:     "55",
				})
			})
		})

		Convey("Then patch dimension option with a valid order returns ok", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/order", "value": 0}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)

			Convey("And the expected database calls are performed to update order", func() {
				expectedOrder := 0
				validateDimensionUpdate(mockedDataStore, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					Option:     "55",
					Order:      &expectedOrder,
				})
			})
		})

		Convey("Then patch dimension option with a valid order and node_id returns ok", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/order", "value": 0},
				{"op": "add", "path": "/node_id", "value": "11"}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)

			Convey("And the expected database calls are performed to update node_id and order", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 3)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[1].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[2].ID, ShouldEqual, "123")

				expectedOrder := 0
				So(mockedDataStore.UpdateDimensionNodeIDAndOrderCalls(), ShouldHaveLength, 2)
				So(mockedDataStore.UpdateDimensionNodeIDAndOrderCalls()[0].Dimension, ShouldResemble, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					Option:     "55",
					Order:      &expectedOrder,
				})
				So(mockedDataStore.UpdateDimensionNodeIDAndOrderCalls()[1].Dimension, ShouldResemble, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					NodeID:     "11",
					Option:     "55",
				})
			})
		})
	})
}

// Deprecated
func TestAddNodeIDToDimensionReturnsNotFound(t *testing.T) {
	t.Parallel()

	Convey("Given a mocked Dataset API that fails to update dimension node ID due to DimensionNodeNotFound error", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			UpdateDimensionNodeIDAndOrderFunc: func(event *models.DimensionOption) error {
				return errs.ErrDimensionNodeNotFound
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		Convey("Add node id to a dimension returns status not found", func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusNotFound)

			Convey("And the expected database calls are performed to update nodeID", func() {
				validateDimensionUpdate(mockedDataStore, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					NodeID:     "11",
					Option:     "55",
					Order:      nil,
				})
			})
		})
	})
}

func TestPatchOptionReturnsNotFound(t *testing.T) {
	t.Parallel()

	Convey("Given a Dataset API instance with a mocked data store that fails to update dimension node ID due to DimensionNodeNotFound error", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
			UpdateDimensionNodeIDAndOrderFunc: func(event *models.DimensionOption) error {
				return errs.ErrDimensionNodeNotFound
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		Convey("Then patch dimension option returns status not found", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/node_id", "value": "11"}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusNotFound)

			Convey("And the expected database calls are performed to update nodeID", func() {
				validateDimensionUpdate(mockedDataStore, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					NodeID:     "11",
					Option:     "55",
					Order:      nil,
				})
			})
		})
	})
}

func TestPatchOptionReturnsBadRequest(t *testing.T) {
	t.Parallel()

	Convey("Given a Dataset API instance with a mocked datastore GetInstance", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{State: models.CreatedState}, nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		bodies := map[string]io.Reader{
			"Then patch dimension option with an invalid body returns bad request":                            strings.NewReader(`wrong`),
			"Then patch dimension option with a patch containing an unsupported method returns bad request":   strings.NewReader(`[{"op": "remove", "path": "/node_id"}]`),
			"Then patch dimension option with an unexpected path returns bad request":                         strings.NewReader(`[{"op": "add", "path": "unexpected", "value": "11"}]`),
			"Then patch dimension option with an unexpected value type for /node_id path returns bad request": strings.NewReader(`[{"op": "add", "path": "/node_id", "value": 123.321}]`),
			"Then patch dimension option with an unexpected value type for /order path returns bad request":   strings.NewReader(`[{"op": "add", "path": "/order", "value": "notAnOrder"}]`),
		}

		for msg, body := range bodies {
			Convey(msg, func() {
				r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
				So(err, ShouldBeNil)

				datasetAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
			})
		}
	})
}

// Deprecated
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
	})
}

func TestPatchOptionReturnsInternalError(t *testing.T) {
	t.Parallel()

	body := strings.NewReader(`[
		{"op": "add", "path": "/order", "value": 0},
		{"op": "add", "path": "/node_id", "value": "11"}
	]`)

	Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return nil, errs.ErrInternalServer
			},
		}

		r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
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
	})

	Convey("Given an internal error is returned from mongo GetInstance on the second call, then response returns an internal error", t, func() {
		mockedDataStore := &storetest.StorerMock{}
		mockedDataStore.GetInstanceFunc = func(ID string) (*models.Instance, error) {
			if len(mockedDataStore.GetInstanceCalls()) == 1 {
				return &models.Instance{State: models.CreatedState}, nil
			}
			return nil, errs.ErrInternalServer
		}

		r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
	})

}

// Deprecated
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
	})
}

func TestPatchOptionReturnsForbidden(t *testing.T) {
	t.Parallel()
	Convey("Patch dimension option of a published instance returns forbidden", t, func() {
		body := strings.NewReader(`[
			{"op": "add", "path": "/order", "value": 0},
			{"op": "add", "path": "/node_id", "value": "11"}
		]`)
		r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
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
	})
}

// Deprecated
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
	})
}

func TestPatchOptionReturnsUnauthorized(t *testing.T) {
	t.Parallel()
	Convey("Patch option of an instance returns unauthorized", t, func() {
		body := strings.NewReader(`[
			{"op": "add", "path": "/order", "value": 0},
			{"op": "add", "path": "/node_id", "value": "11"}
		]`)
		r, err := http.NewRequest(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
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
	})
}

func TestAddDimensionToInstanceReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Add a dimension to an instance returns ok", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test", "order": 1}`)
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
		So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
		So(mockedDataStore.GetInstanceCalls()[1].ID, ShouldEqual, "123")

		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)
		So(mockedDataStore.AddDimensionToInstanceCalls()[0].Dimension.CodeList, ShouldEqual, "123-456")
		So(mockedDataStore.AddDimensionToInstanceCalls()[0].Dimension.Name, ShouldEqual, "test")
		So(*mockedDataStore.AddDimensionToInstanceCalls()[0].Dimension.Order, ShouldEqual, 1)
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

	Convey("Given an internal error is returned from mongo GetInstance, then response returns an internal error", t, func() {
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

	return api.Setup(ctx, cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedGeneratedDownloads, datasetPermissions, permissions)
}

func getAuthorisationHandlerMock() *mocks.AuthHandlerMock {
	return &mocks.AuthHandlerMock{
		Required: &mocks.PermissionCheckCalls{Calls: 0},
	}
}
