package dimension_test

import (
	"context"
	"fmt"
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
	testETag    = "testETag"
	testIfMatch = "testIfMatch"
	testLockID  = "testLockID"
	AnyETag     = "*"
)

func createRequestWithToken(method, url string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequest(method, url, body)
	ctx := r.Context()
	ctx = dprequest.SetCaller(ctx, "someone@ons.gov.uk")
	r = r.WithContext(ctx)
	return r, err
}

func validateDimensionUpdate(mockedDataStore *storetest.StorerMock, expected *models.DimensionOption, expectedIfMatch string) {
	// Gets called twice as there is a check wrapper around this route which
	// checks the instance is not published before entering handler
	So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 2)
	So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, expected.InstanceID)
	So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, expectedIfMatch)
	So(mockedDataStore.GetInstanceCalls()[1].ID, ShouldEqual, expected.InstanceID)
	So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, ShouldEqual, expectedIfMatch)
	validateLock(mockedDataStore, expected.InstanceID)

	So(mockedDataStore.UpdateETagForNodeIDAndOrderCalls(), ShouldHaveLength, 1)
	So(mockedDataStore.UpdateETagForNodeIDAndOrderCalls()[0].CurrentInstance.InstanceID, ShouldEqual, expected.InstanceID)
	So(mockedDataStore.UpdateETagForNodeIDAndOrderCalls()[0].ETagSelector, ShouldEqual, expectedIfMatch)

	So(mockedDataStore.UpdateDimensionNodeIDAndOrderCalls(), ShouldHaveLength, 1)
	So(mockedDataStore.UpdateDimensionNodeIDAndOrderCalls()[0].Dimension, ShouldResemble, expected)
}

func validateLock(mockedDataStore *storetest.StorerMock, expectedInstanceID string) {
	So(mockedDataStore.AcquireInstanceLockCalls(), ShouldHaveLength, 1)
	So(mockedDataStore.AcquireInstanceLockCalls()[0].InstanceID, ShouldEqual, "123")
	So(mockedDataStore.UnlockInstanceCalls(), ShouldHaveLength, 1)
	So(mockedDataStore.UnlockInstanceCalls()[0].LockID, ShouldEqual, testLockID)
}

func storeMockWithLock(expectFirstGetUnlocked bool) (*storetest.StorerMock, *bool) {
	isLocked := false
	numGetCall := 0
	return &storetest.StorerMock{
		AcquireInstanceLockFunc: func(ctx context.Context, instanceID string) (string, error) {
			isLocked = true
			return testLockID, nil
		},
		UnlockInstanceFunc: func(lockID string) error {
			isLocked = false
			return nil
		},
		GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
			if expectFirstGetUnlocked {
				if numGetCall > 0 {
					So(isLocked, ShouldBeTrue)
				} else {
					So(isLocked, ShouldBeFalse)
				}
			}
			numGetCall++
			return &models.Instance{
				InstanceID: ID,
				State:      models.CreatedState,
				ETag:       testETag,
			}, nil
		},
	}, &isLocked
}

// Deprecated
func TestAddNodeIDToDimensionReturnsOK(t *testing.T) {
	t.Parallel()

	Convey("Given a dataset API with a successful store mock and auth", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForNodeIDAndOrderFunc = func(currentInstance *models.Instance, nodeID string, order *int, eTagSelector string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.UpdateDimensionNodeIDAndOrderFunc = func(dimension *models.DimensionOption) error {
			So(*isLocked, ShouldBeTrue)
			return nil
		}
		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		Convey("When a PUT request to update the nodeID for an option is made, with a valid If-Match header", func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
			r.Header.Add("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called", func() {
				validateDimensionUpdate(mockedDataStore, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					NodeID:     "11",
					Option:     "55",
					Order:      nil,
				}, testIfMatch)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When a PUT request to update the nodeID for an option is made, without an If-Match header", func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called, with the '*' wildchar when validting the provided If-Match value", func() {
				validateDimensionUpdate(mockedDataStore, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					NodeID:     "11",
					Option:     "55",
					Order:      nil,
				}, AnyETag)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func TestPatchOptionReturnsOK(t *testing.T) {
	t.Parallel()

	Convey("Given a Dataset API instance with a mocked data store", t, func() {
		w := httptest.NewRecorder()

		numUpdateCall := 0

		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForNodeIDAndOrderFunc = func(currentInstance *models.Instance, nodeID string, order *int, eTagSelector string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			newETag := fmt.Sprintf("%s_%d", testETag, numUpdateCall)
			numUpdateCall++
			return newETag, nil
		}
		mockedDataStore.UpdateDimensionNodeIDAndOrderFunc = func(dimension *models.DimensionOption) error {
			So(*isLocked, ShouldBeTrue)
			return nil
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		Convey("Then patch dimension option with a valid node_id returns ok", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/node_id", "value": "11"}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)
			expectedETag := fmt.Sprintf("%s_0", testETag)
			So(w.Header().Get("ETag"), ShouldEqual, expectedETag)

			Convey("And the expected database calls are performed to update node_id", func() {
				validateDimensionUpdate(mockedDataStore, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					NodeID:     "11",
					Option:     "55",
				}, testIfMatch)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("Then patch dimension option with a valid node_id, without an If-Match header, returns ok", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/node_id", "value": "11"}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)
			expectedETag := fmt.Sprintf("%s_0", testETag)
			So(w.Header().Get("ETag"), ShouldEqual, expectedETag)

			Convey("And the expected database calls are performed to update node_id without checking any eTag", func() {
				validateDimensionUpdate(mockedDataStore, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					NodeID:     "11",
					Option:     "55",
				}, AnyETag)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("Then patch dimension option with a valid order returns ok", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/order", "value": 0}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)
			expectedETag := fmt.Sprintf("%s_0", testETag)
			So(w.Header().Get("ETag"), ShouldEqual, expectedETag)

			Convey("And the expected database calls are performed to update order", func() {
				expectedOrder := 0
				validateDimensionUpdate(mockedDataStore, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					Option:     "55",
					Order:      &expectedOrder,
				}, testIfMatch)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("Then patch dimension option with a valid order and node_id returns ok", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/order", "value": 0},
				{"op": "add", "path": "/node_id", "value": "11"}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)

			expectedETag0 := fmt.Sprintf("%s_0", testETag)
			expectedETag1 := fmt.Sprintf("%s_1", testETag)

			datasetAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusOK)
			So(w.Header().Get("ETag"), ShouldEqual, expectedETag1)

			Convey("And the expected database calls are performed to update node_id and order", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 3)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.GetInstanceCalls()[1].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.GetInstanceCalls()[2].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[2].ETagSelector, ShouldEqual, expectedETag0)

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
				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

// Deprecated
func TestAddNodeIDToDimensionReturnsNotFound(t *testing.T) {
	t.Parallel()

	Convey("Given a mocked Dataset API that fails to update dimension node ID due to DimensionNodeNotFound error", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForNodeIDAndOrderFunc = func(currentInstance *models.Instance, nodeID string, order *int, eTagSelector string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.UpdateDimensionNodeIDAndOrderFunc = func(dimension *models.DimensionOption) error {
			So(*isLocked, ShouldBeTrue)
			return errs.ErrDimensionNodeNotFound
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		Convey("Add node id to a dimension returns status not found", func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
			r.Header.Set("If-Match", testIfMatch)
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
				}, testIfMatch)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func TestPatchOptionReturnsNotFound(t *testing.T) {
	t.Parallel()

	Convey("Given a Dataset API instance with a mocked data store that fails to update dimension node ID due to DimensionNodeNotFound error", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForNodeIDAndOrderFunc = func(currentInstance *models.Instance, nodeID string, order *int, eTagSelector string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.UpdateDimensionNodeIDAndOrderFunc = func(dimension *models.DimensionOption) error {
			So(*isLocked, ShouldBeTrue)
			return errs.ErrDimensionNodeNotFound
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		Convey("Then patch dimension option returns status not found", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/node_id", "value": "11"}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			r.Header.Set("If-Match", testIfMatch)
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
				}, testIfMatch)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func TestPatchOptionReturnsBadRequest(t *testing.T) {
	t.Parallel()

	Convey("Given a Dataset API instance with a mocked datastore GetInstance", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(false)
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
				So(*isLocked, ShouldBeFalse)
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
			GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
				return nil, errs.ErrInternalServer
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
				return &models.Instance{State: "gobbledygook"}, nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
	})
}

func TestPatchOptionReturnsInternalError(t *testing.T) {
	t.Parallel()

	body := strings.NewReader(`[
		{"op": "add", "path": "/order", "value": 0},
		{"op": "add", "path": "/node_id", "value": "11"}
	]`)

	Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
			return nil, errs.ErrInternalServer
		}

		r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
		So(*isLocked, ShouldBeFalse)
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
			return &models.Instance{State: "gobbledygook"}, nil
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
		So(*isLocked, ShouldBeFalse)
	})

	Convey("Given an internal error is returned from mongo GetInstance on the second call, then response returns an internal error", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
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
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 2)
		So(*isLocked, ShouldBeFalse)
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
			GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
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
			GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
	})
}

// Deprecated
func TestAddNodeIDToDimensionReturnsUnauthorized(t *testing.T) {
	t.Parallel()
	Convey("Add node id to a dimension of an instance returns unauthorized", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
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

		mockedDataStore := &storetest.StorerMock{}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
	})
}

func TestAddDimensionToInstanceReturnsOk(t *testing.T) {
	t.Parallel()

	bodyStr := `{"value":"24", "code_list":"123-456", "dimension": "test", "order": 1}`
	expectedOrder := 1
	expected := &models.CachedDimensionOption{
		InstanceID: "123",
		CodeList:   "123-456",
		Name:       "test",
		Order:      &expectedOrder,
	}

	Convey("Given a dataset API with a successful store mock and auth", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForOptionsFunc = func(currentInstance *models.Instance, option *models.CachedDimensionOption, eTagSelector string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.AddDimensionToInstanceFunc = func(dimension *models.CachedDimensionOption) error {
			So(*isLocked, ShouldBeTrue)
			return nil
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		Convey("When a POST request to add dimensions to an instance resource is made, with a valid If-Match header", func() {
			json := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("POST", "http://localhost:22000/instances/123/dimensions", json)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called", func() {
				// Gets called twice as there is a check wrapper around this route which
				// checks the instance is not published before entering handler
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 2)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.GetInstanceCalls()[1].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.UpdateETagForOptionsCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.UpdateETagForOptionsCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.UpdateETagForOptionsCalls()[0].Option, ShouldResemble, expected)
				So(mockedDataStore.AddDimensionToInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.AddDimensionToInstanceCalls()[0].Dimension, ShouldResemble, expected)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When a POST request to add dimensions to an instance resource is made, without an If-Match header", func() {
			json := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("POST", "http://localhost:22000/instances/123/dimensions", json)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called, with the '*' wildchar when validting the provided If-Match value", func() {
				// Gets called twice as there is a check wrapper around this route which
				// checks the instance is not published before entering handler
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 2)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, AnyETag)
				So(mockedDataStore.GetInstanceCalls()[1].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, ShouldEqual, AnyETag)
				So(mockedDataStore.UpdateETagForOptionsCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.UpdateETagForOptionsCalls()[0].ETagSelector, ShouldEqual, AnyETag)
				So(mockedDataStore.UpdateETagForOptionsCalls()[0].Option, ShouldResemble, expected)
				So(mockedDataStore.AddDimensionToInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.AddDimensionToInstanceCalls()[0].Dimension, ShouldResemble, expected)
			})
		})
	})
}

func TestAddDimensionToInstanceReturnsNotFound(t *testing.T) {
	t.Parallel()
	Convey("Add a dimension to an instance returns not found", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		expected := &models.CachedDimensionOption{
			InstanceID: "123",
			CodeList:   "123-456",
			Name:       "test",
		}

		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForOptionsFunc = func(currentInstance *models.Instance, option *models.CachedDimensionOption, eTagSelector string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.AddDimensionToInstanceFunc = func(dimension *models.CachedDimensionOption) error {
			So(*isLocked, ShouldBeTrue)
			return errs.ErrDimensionNodeNotFound
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDimensionNodeNotFound.Error())
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 2)
		So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
		So(mockedDataStore.GetInstanceCalls()[1].ID, ShouldEqual, "123")

		validateLock(mockedDataStore, "123")

		So(mockedDataStore.UpdateETagForOptionsCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpdateETagForOptionsCalls()[0].ETagSelector, ShouldEqual, "*")
		So(mockedDataStore.UpdateETagForOptionsCalls()[0].Option, ShouldResemble, expected)

		So(mockedDataStore.AddDimensionToInstanceCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.AddDimensionToInstanceCalls()[0].Dimension, ShouldResemble, expected)
		So(*isLocked, ShouldBeFalse)
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
			GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrResourcePublished.Error())
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
	})
}

func TestAddDimensionToInstanceReturnsUnauthorized(t *testing.T) {
	t.Parallel()
	Convey("Add a dimension to a instance returns unauthorized", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := http.NewRequest("POST", "http://localhost:21800/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		So(w.Body.String(), ShouldContainSubstring, "unauthenticated request")
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
			GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
				return nil, errs.ErrInternalServer
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
				return &models.Instance{State: "gobbledygook"}, nil
			},
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
	})
}

func TestGetDimensionsReturnsOk(t *testing.T) {
	t.Parallel()

	Convey("Given a dataset API with a successful store mock and auth", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetDimensionsFromInstanceFunc = func(ctx context.Context, id string, offset, limit int) ([]*models.DimensionOption, int, error) {
			So(*isLocked, ShouldBeTrue)
			return []*models.DimensionOption{}, 0, nil
		}
		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		Convey("When a GET request to retrieve an instance resource is made, with a valid If-Match header", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.GetDimensionsFromInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].Offset, ShouldEqual, 0)
				So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].Limit, ShouldEqual, 20)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When a GET request to retrieve an instance resource is made, without an If-Match header", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, AnyETag)
				So(mockedDataStore.GetDimensionsFromInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].Offset, ShouldEqual, 0)
				So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].Limit, ShouldEqual, 20)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func TestGetDimensionsReturnsNotFound(t *testing.T) {
	t.Parallel()
	Convey("Get dimensions returns not found", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetDimensionsFromInstanceFunc = func(ctx context.Context, id string, offset, limit int) ([]*models.DimensionOption, int, error) {
			So(*isLocked, ShouldBeTrue)
			return nil, 0, errs.ErrDimensionNodeNotFound
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrDimensionNodeNotFound.Error())
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
		So(mockedDataStore.GetDimensionsFromInstanceCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].ID, ShouldEqual, "123")
		validateLock(mockedDataStore, "123")
		So(*isLocked, ShouldBeFalse)
	})
}

func TestGetDimensionsReturnsConflict(t *testing.T) {
	t.Parallel()
	Convey("Get dimensions returns conflict", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		r.Header.Set("If-Match", "wrong")
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
			So(*isLocked, ShouldBeTrue)
			return nil, errs.ErrInstanceConflict
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusConflict)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceConflict.Error())
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
		So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, "wrong")
		validateLock(mockedDataStore, "123")
		So(*isLocked, ShouldBeFalse)
	})
}

func TestGetDimensionsAndOptionsReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
			So(*isLocked, ShouldBeTrue)
			return nil, errs.ErrInternalServer
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())

		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
		validateLock(mockedDataStore, "123")
		So(*isLocked, ShouldBeFalse)
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
			So(*isLocked, ShouldBeTrue)
			return &models.Instance{State: "gobbly gook"}, nil
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
		validateLock(mockedDataStore, "123")
		So(*isLocked, ShouldBeFalse)
	})
}

func TestGetUniqueDimensionAndOptionsReturnsOk(t *testing.T) {
	t.Parallel()

	Convey("Given a dataset API with a successful store mock and auth", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetUniqueDimensionAndOptionsFunc = func(ctx context.Context, ID string, dimension string, offset int, limit int) ([]*string, int, error) {
			So(*isLocked, ShouldBeTrue)
			return []*string{}, 0, nil
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		Convey("When a GET request to retrieve an instance resource is made, with a valid If-Match header", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.GetUniqueDimensionAndOptionsCalls(), ShouldHaveLength, 1)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When a GET request to retrieve an instance resource is made, without an If-Match header", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called, with the '*' wildchar when validting the provided If-Match value", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, AnyETag)
				So(mockedDataStore.GetUniqueDimensionAndOptionsCalls(), ShouldHaveLength, 1)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func TestGetUniqueDimensionAndOptionsReturnsNotFound(t *testing.T) {
	t.Parallel()
	Convey("Get all unique dimensions returns not found", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetUniqueDimensionAndOptionsFunc = func(ctx context.Context, ID string, dimension string, offset int, limit int) ([]*string, int, error) {
			So(*isLocked, ShouldBeTrue)
			return nil, 0, errs.ErrInstanceNotFound
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
		So(mockedDataStore.GetUniqueDimensionAndOptionsCalls(), ShouldHaveLength, 1)
		validateLock(mockedDataStore, "123")
		So(*isLocked, ShouldBeFalse)
	})
}

func TestGetUniqueDimensionAndOptionsReturnsConflict(t *testing.T) {
	t.Parallel()
	Convey("Get all unique dimensions returns conflict", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		r.Header.Set("If-Match", "wrong")
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
			return nil, errs.ErrInstanceConflict
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusConflict)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceConflict.Error())
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
		So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, "wrong")
		validateLock(mockedDataStore, "123")
		So(*isLocked, ShouldBeFalse)
	})
}

func TestGetUniqueDimensionAndOptionsReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
			So(*isLocked, ShouldBeTrue)
			return nil, errs.ErrInternalServer
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
		validateLock(mockedDataStore, "123")
		So(*isLocked, ShouldBeFalse)
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(ID string, eTagSelector string) (*models.Instance, error) {
			So(*isLocked, ShouldBeTrue)
			return &models.Instance{State: "gobbly gook"}, nil
		}

		datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
		validateLock(mockedDataStore, "123")
		So(*isLocked, ShouldBeFalse)
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
