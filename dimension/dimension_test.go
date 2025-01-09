package dimension_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
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
	dprequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/gorilla/mux"
	"github.com/smartystreets/goconvey/convey"
)

var (
	codeListAPIURL        = &neturl.URL{Scheme: "http", Host: "localhost:22400"}
	datasetAPIURL         = &neturl.URL{Scheme: "http", Host: "localhost:22000"}
	downloadServiceURL    = &neturl.URL{Scheme: "http", Host: "localhost:23600"}
	importAPIURL          = &neturl.URL{Scheme: "http", Host: "localhost:21800"}
	websiteURL            = &neturl.URL{Scheme: "http", Host: "localhost:20000"}
	urlBuilder            = url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL)
	mu                    sync.Mutex
	testContext           = context.Background()
	testETag              = "testETag"
	testIfMatch           = "testIfMatch"
	testLockID            = "testLockID"
	AnyETag               = "*"
	testMaxRequestOptions = 10
)

func createRequestWithToken(method, requestURL string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequest(method, requestURL, body)
	ctx := r.Context()
	ctx = dprequest.SetCaller(ctx, "someone@ons.gov.uk")
	r = r.WithContext(ctx)
	return r, err
}

func validateDimensionUpdates(mockedDataStore *storetest.StorerMock, expected []*models.DimensionOption, expectedIfMatch string) {
	// Gets called twice as there is a check wrapper around this route which
	// checks the instance is not published before entering handler
	convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 2)
	convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, expected[0].InstanceID)
	convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, expectedIfMatch)
	convey.So(mockedDataStore.GetInstanceCalls()[1].ID, convey.ShouldEqual, expected[0].InstanceID)
	convey.So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, convey.ShouldEqual, expectedIfMatch)
	validateLock(mockedDataStore, expected[0].InstanceID)

	convey.So(mockedDataStore.UpdateETagForOptionsCalls(), convey.ShouldHaveLength, 1)
	convey.So(mockedDataStore.UpdateETagForOptionsCalls()[0].CurrentInstance.InstanceID, convey.ShouldEqual, expected[0].InstanceID)
	convey.So(mockedDataStore.UpdateETagForOptionsCalls()[0].ETagSelector, convey.ShouldEqual, expectedIfMatch)

	convey.So(mockedDataStore.UpdateDimensionsNodeIDAndOrderCalls(), convey.ShouldHaveLength, 1)
	convey.So(mockedDataStore.UpdateDimensionsNodeIDAndOrderCalls()[0].Updates, convey.ShouldResemble, expected)
}

// validateDimensionsUpserted validates that the provided expectedUpserts were performed sequentially in the provided order
func validateDimensionsUpserted(mockedDataStore *storetest.StorerMock, instanceID string, expectedUpserts [][]*models.CachedDimensionOption, expectedIfMatch []string) {
	convey.So(len(expectedUpserts), convey.ShouldEqual, len(expectedIfMatch))

	// Gets called is called an extra time, as there is a check wrapper around this route which
	// checks the instance is not published before entering handler
	convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, len(expectedUpserts)+1)
	convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, instanceID)
	convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, expectedIfMatch[0])
	for i := range expectedUpserts {
		convey.So(mockedDataStore.GetInstanceCalls()[i+1].ID, convey.ShouldEqual, instanceID)
		convey.So(mockedDataStore.GetInstanceCalls()[i+1].ETagSelector, convey.ShouldEqual, expectedIfMatch[i])
	}

	// validate UpdateETag calls
	convey.So(mockedDataStore.UpdateETagForOptionsCalls(), convey.ShouldHaveLength, len(expectedUpserts))
	for i := range expectedUpserts {
		convey.So(mockedDataStore.UpdateETagForOptionsCalls()[i].CurrentInstance.InstanceID, convey.ShouldEqual, instanceID)
		convey.So(mockedDataStore.UpdateETagForOptionsCalls()[i].ETagSelector, convey.ShouldEqual, expectedIfMatch[i])
	}

	// validate Upsert calls
	convey.So(mockedDataStore.UpsertDimensionsToInstanceCalls(), convey.ShouldHaveLength, len(expectedUpserts))
	for i, expected := range expectedUpserts {
		convey.So(mockedDataStore.UpsertDimensionsToInstanceCalls()[i].Dimensions, convey.ShouldResemble, expected)
	}
}

func validateLock(mockedDataStore *storetest.StorerMock, expectedInstanceID string) {
	convey.So(mockedDataStore.AcquireInstanceLockCalls(), convey.ShouldHaveLength, 1)
	convey.So(mockedDataStore.AcquireInstanceLockCalls()[0].InstanceID, convey.ShouldEqual, expectedInstanceID)
	convey.So(mockedDataStore.UnlockInstanceCalls(), convey.ShouldHaveLength, 1)
	convey.So(mockedDataStore.UnlockInstanceCalls()[0].LockID, convey.ShouldEqual, testLockID)
}

func storeMockWithLock(expectFirstGetUnlocked bool) (storerTestMock *storetest.StorerMock, isLockedPointer *bool) {
	isLocked := false
	numGetCall := 0
	storerTestMock = &storetest.StorerMock{
		AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
			isLocked = true
			return testLockID, nil
		},
		UnlockInstanceFunc: func(context.Context, string) {
			isLocked = false
		},
		GetInstanceFunc: func(_ context.Context, ID string, _ string) (*models.Instance, error) {
			if expectFirstGetUnlocked {
				if numGetCall > 0 {
					convey.So(isLocked, convey.ShouldBeTrue)
				} else {
					convey.So(isLocked, convey.ShouldBeFalse)
				}
			}
			numGetCall++
			return &models.Instance{
				InstanceID: ID,
				State:      models.CreatedState,
				ETag:       testETag,
			}, nil
		},
	}
	return storerTestMock, &isLocked
}

// Deprecated
func TestAddNodeIDToDimensionReturnsOK(t *testing.T) {
	t.Parallel()

	convey.Convey("Given a dataset API with a successful store mock and auth", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.UpdateDimensionsNodeIDAndOrderFunc = func(context.Context, []*models.DimensionOption) error {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return nil
		}
		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		convey.Convey("When a PUT request to update the nodeID for an option is made, with a valid If-Match header", func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
			r.Header.Add("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called", func() {
				validateDimensionUpdates(mockedDataStore, []*models.DimensionOption{
					{
						InstanceID: "123",
						Name:       "age",
						NodeID:     "11",
						Option:     "55",
						Order:      nil,
					},
				}, testIfMatch)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When a PUT request to update the nodeID for an option is made, without an If-Match header", func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called, with the '*' wildchar when validting the provided If-Match value", func() {
				validateDimensionUpdates(mockedDataStore, []*models.DimensionOption{
					{
						InstanceID: "123",
						Name:       "age",
						NodeID:     "11",
						Option:     "55",
						Order:      nil,
					},
				}, AnyETag)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func TestPatchOptionReturnsOK(t *testing.T) {
	t.Parallel()

	convey.Convey("Given a Dataset API instance with a mocked data store", t, func() {
		w := httptest.NewRecorder()

		numUpdateCall := 0

		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			newETag := fmt.Sprintf("%s_%d", testETag, numUpdateCall)
			numUpdateCall++
			return newETag, nil
		}
		mockedDataStore.UpdateDimensionsNodeIDAndOrderFunc = func(context.Context, []*models.DimensionOption) error {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return nil
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		convey.Convey("Then patch dimension option with a valid node_id returns ok", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/node_id", "value": "11"}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
			expectedETag := fmt.Sprintf("%s_0", testETag)
			convey.So(w.Header().Get("ETag"), convey.ShouldEqual, expectedETag)

			convey.Convey("And the expected database calls are performed to update node_id", func() {
				validateDimensionUpdates(mockedDataStore, []*models.DimensionOption{
					{
						InstanceID: "123",
						Name:       "age",
						NodeID:     "11",
						Option:     "55",
					},
				}, testIfMatch)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("Then patch dimension option with a valid node_id, without an If-Match header, returns ok", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/node_id", "value": "11"}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			convey.So(err, convey.ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
			expectedETag := fmt.Sprintf("%s_0", testETag)
			convey.So(w.Header().Get("ETag"), convey.ShouldEqual, expectedETag)

			convey.Convey("And the expected database calls are performed to update node_id without checking any eTag", func() {
				validateDimensionUpdates(mockedDataStore, []*models.DimensionOption{
					{
						InstanceID: "123",
						Name:       "age",
						NodeID:     "11",
						Option:     "55",
					},
				}, AnyETag)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("Then patch dimension option with a valid order returns ok", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/order", "value": 0}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
			expectedETag := fmt.Sprintf("%s_0", testETag)
			convey.So(w.Header().Get("ETag"), convey.ShouldEqual, expectedETag)

			convey.Convey("And the expected database calls are performed to update order", func() {
				expectedOrder := 0
				validateDimensionUpdates(mockedDataStore, []*models.DimensionOption{
					{
						InstanceID: "123",
						Name:       "age",
						Option:     "55",
						Order:      &expectedOrder,
					},
				}, testIfMatch)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("Then patch dimension option with a valid order and node_id returns ok", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/order", "value": 0},
				{"op": "add", "path": "/node_id", "value": "11"}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)

			expectedETag0 := fmt.Sprintf("%s_0", testETag)
			expectedETag1 := fmt.Sprintf("%s_1", testETag)

			datasetAPI.Router.ServeHTTP(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
			convey.So(w.Header().Get("ETag"), convey.ShouldEqual, expectedETag1)

			convey.Convey("And the expected database calls are performed to update node_id and order", func() {
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 3)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.GetInstanceCalls()[1].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.GetInstanceCalls()[2].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[2].ETagSelector, convey.ShouldEqual, expectedETag0)

				expectedOrder := 0
				convey.So(mockedDataStore.UpdateDimensionsNodeIDAndOrderCalls(), convey.ShouldHaveLength, 2)
				convey.So(mockedDataStore.UpdateDimensionsNodeIDAndOrderCalls()[0].Updates[0], convey.ShouldResemble, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					Option:     "55",
					Order:      &expectedOrder,
				})
				convey.So(mockedDataStore.UpdateDimensionsNodeIDAndOrderCalls()[1].Updates[0], convey.ShouldResemble, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					NodeID:     "11",
					Option:     "55",
				})
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

// Deprecated
func TestAddNodeIDToDimensionReturnsNotFound(t *testing.T) {
	t.Parallel()

	convey.Convey("Given a mocked Dataset API that fails to update dimension node ID due to DimensionNodeNotFound error", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.UpdateDimensionsNodeIDAndOrderFunc = func(context.Context, []*models.DimensionOption) error {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return errs.ErrDimensionNodeNotFound
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		convey.Convey("Add node id to a dimension returns status not found", func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)

			convey.Convey("And the expected database calls are performed to update nodeID", func() {
				validateDimensionUpdates(mockedDataStore, []*models.DimensionOption{
					{
						InstanceID: "123",
						Name:       "age",
						NodeID:     "11",
						Option:     "55",
						Order:      nil,
					},
				}, testIfMatch)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func TestPatchOptionReturnsNotFound(t *testing.T) {
	t.Parallel()

	convey.Convey("Given a Dataset API instance with a mocked data store that fails to update dimension node ID due to DimensionNodeNotFound error", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.UpdateDimensionsNodeIDAndOrderFunc = func(context.Context, []*models.DimensionOption) error {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return errs.ErrDimensionNodeNotFound
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		convey.Convey("Then patch dimension option returns status not found", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/node_id", "value": "11"}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)

			convey.Convey("And the expected database calls are performed to update nodeID", func() {
				validateDimensionUpdates(mockedDataStore, []*models.DimensionOption{
					{
						InstanceID: "123",
						Name:       "age",
						NodeID:     "11",
						Option:     "55",
						Order:      nil,
					},
				}, testIfMatch)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func TestPatchOptionReturnsBadRequest(t *testing.T) {
	t.Parallel()

	convey.Convey("Given a Dataset API instance with a mocked datastore GetInstance", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(false)
		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		bodies := map[string]io.Reader{
			"Then patch dimension option with an invalid body returns bad request":                            strings.NewReader(`wrong`),
			"Then patch dimension option with a patch containing an unsupported method returns bad request":   strings.NewReader(`[{"op": "remove", "path": "/node_id"}]`),
			"Then patch dimension option with an unexpected path returns bad request":                         strings.NewReader(`[{"op": "add", "path": "unexpected", "value": "11"}]`),
			"Then patch dimension option with an unexpected value type for /node_id path returns bad request": strings.NewReader(`[{"op": "add", "path": "/node_id", "value": 123.321}]`),
			"Then patch dimension option with an unexpected value type for /order path returns bad request":   strings.NewReader(`[{"op": "add", "path": "/order", "value": "notAnOrder"}]`),
		}

		for msg, body := range bodies {
			convey.Convey(msg, func() {
				r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
				convey.So(err, convey.ShouldBeNil)

				datasetAPI.Router.ServeHTTP(w, r)
				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		}
	})
}

// Deprecated
func TestAddNodeIDToDimensionReturnsInternalError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return nil, errs.ErrInternalServer
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
	})

	convey.Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return &models.Instance{State: "gobbledygook"}, nil
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
	})
}

func TestPatchOptionReturnsInternalError(t *testing.T) {
	t.Parallel()

	body := strings.NewReader(`[
		{"op": "add", "path": "/order", "value": 0},
		{"op": "add", "path": "/node_id", "value": "11"}
	]`)

	convey.Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			return nil, errs.ErrInternalServer
		}

		r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
		convey.So(*isLocked, convey.ShouldBeFalse)
	})

	convey.Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			return &models.Instance{State: "gobbledygook"}, nil
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
		convey.So(*isLocked, convey.ShouldBeFalse)
	})

	convey.Convey("Given an internal error is returned from mongo GetInstance on the second call, then response returns an internal error", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			if len(mockedDataStore.GetInstanceCalls()) == 1 {
				return &models.Instance{State: models.CreatedState}, nil
			}
			return nil, errs.ErrInternalServer
		}

		r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 2)
		convey.So(*isLocked, convey.ShouldBeFalse)
	})
}

// Deprecated
func TestAddNodeIDToDimensionReturnsForbidden(t *testing.T) {
	t.Parallel()
	convey.Convey("Add node id to a dimension of a published instance returns forbidden", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusForbidden)
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
	})
}

func TestPatchOptionReturnsForbidden(t *testing.T) {
	t.Parallel()
	convey.Convey("Patch dimension option of a published instance returns forbidden", t, func() {
		body := strings.NewReader(`[
			{"op": "add", "path": "/order", "value": 0},
			{"op": "add", "path": "/node_id", "value": "11"}
		]`)
		r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusForbidden)
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
	})
}

// Deprecated
func TestAddNodeIDToDimensionReturnsUnauthorized(t *testing.T) {
	t.Parallel()
	convey.Convey("Add node id to a dimension of an instance returns unauthorized", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", http.NoBody)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusUnauthorized)
	})
}

func TestPatchOptionReturnsUnauthorized(t *testing.T) {
	t.Parallel()
	convey.Convey("Patch option of an instance returns unauthorized", t, func() {
		body := strings.NewReader(`[
			{"op": "add", "path": "/order", "value": 0},
			{"op": "add", "path": "/node_id", "value": "11"}
		]`)
		r, err := http.NewRequest(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusUnauthorized)
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

	convey.Convey("Given a dataset API with a successful store mock and auth", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.UpsertDimensionsToInstanceFunc = func(context.Context, []*models.CachedDimensionOption) error {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return nil
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		convey.Convey("When a POST request to add dimensions to an instance resource is made, with a valid If-Match header", func() {
			json := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("POST", "http://localhost:22000/instances/123/dimensions", json)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called", func() {
				// Gets called twice as there is a check wrapper around this route which
				// checks the instance is not published before entering handler
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 2)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.GetInstanceCalls()[1].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.UpdateETagForOptionsCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.UpdateETagForOptionsCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.UpdateETagForOptionsCalls()[0].Upserts[0], convey.ShouldResemble, expected)
				convey.So(mockedDataStore.UpsertDimensionsToInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.UpsertDimensionsToInstanceCalls()[0].Dimensions[0], convey.ShouldResemble, expected)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When a POST request to add dimensions to an instance resource is made, without an If-Match header", func() {
			json := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("POST", "http://localhost:22000/instances/123/dimensions", json)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called, with the '*' wildchar when validting the provided If-Match value", func() {
				// Gets called twice as there is a check wrapper around this route which
				// checks the instance is not published before entering handler
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 2)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(mockedDataStore.GetInstanceCalls()[1].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(mockedDataStore.UpdateETagForOptionsCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.UpdateETagForOptionsCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(mockedDataStore.UpdateETagForOptionsCalls()[0].Upserts[0], convey.ShouldResemble, expected)
				convey.So(mockedDataStore.UpsertDimensionsToInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.UpsertDimensionsToInstanceCalls()[0].Dimensions[0], convey.ShouldResemble, expected)
			})
		})
	})
}

func TestAddDimensionToInstanceReturnsNotFound(t *testing.T) {
	t.Parallel()
	convey.Convey("Add a dimension to an instance returns not found", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		convey.So(err, convey.ShouldBeNil)

		expected := &models.CachedDimensionOption{
			InstanceID: "123",
			CodeList:   "123-456",
			Name:       "test",
		}

		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.UpsertDimensionsToInstanceFunc = func(context.Context, []*models.CachedDimensionOption) error {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return errs.ErrDimensionNodeNotFound
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrDimensionNodeNotFound.Error())
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 2)
		convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
		convey.So(mockedDataStore.GetInstanceCalls()[1].ID, convey.ShouldEqual, "123")

		validateLock(mockedDataStore, "123")

		convey.So(mockedDataStore.UpdateETagForOptionsCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpdateETagForOptionsCalls()[0].ETagSelector, convey.ShouldEqual, "*")
		convey.So(mockedDataStore.UpdateETagForOptionsCalls()[0].Upserts[0], convey.ShouldResemble, expected)

		convey.So(mockedDataStore.UpsertDimensionsToInstanceCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.UpsertDimensionsToInstanceCalls()[0].Dimensions[0], convey.ShouldResemble, expected)
		convey.So(*isLocked, convey.ShouldBeFalse)
	})
}

func TestAddDimensionToInstanceReturnsForbidden(t *testing.T) {
	t.Parallel()
	convey.Convey("Add a dimension to a published instance returns forbidden", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusForbidden)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrResourcePublished.Error())
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
	})
}

func TestAddDimensionToInstanceReturnsUnauthorized(t *testing.T) {
	t.Parallel()
	convey.Convey("Add a dimension to a instance returns unauthorized", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := http.NewRequest("POST", "http://localhost:21800/instances/123/dimensions", json)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusUnauthorized)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, "unauthenticated request")
	})
}

func TestAddDimensionToInstanceReturnsInternalError(t *testing.T) {
	t.Parallel()

	convey.Convey("Given an internal error is returned from mongo GetInstance, then response returns an internal error", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return nil, errs.ErrInternalServer
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
	})

	convey.Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/dimensions", json)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return &models.Instance{State: "gobbledygook"}, nil
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
	})
}

func TestGetDimensionsReturnsOk(t *testing.T) {
	t.Parallel()

	convey.Convey("Given a dataset API with a successful store mock and auth", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetDimensionsFromInstanceFunc = func(context.Context, string, int, int) ([]*models.DimensionOption, int, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return []*models.DimensionOption{}, 0, nil
		}
		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		convey.Convey("When a GET request to retrieve an instance resource is made, with a valid If-Match header", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called", func() {
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.GetDimensionsFromInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].Offset, convey.ShouldEqual, 0)
				convey.So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].Limit, convey.ShouldEqual, 20)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When a GET request to retrieve an instance resource is made, without an If-Match header", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called", func() {
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(mockedDataStore.GetDimensionsFromInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].Offset, convey.ShouldEqual, 0)
				convey.So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].Limit, convey.ShouldEqual, 20)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func TestGetDimensionsReturnsNotFound(t *testing.T) {
	t.Parallel()
	convey.Convey("Get dimensions returns not found", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetDimensionsFromInstanceFunc = func(context.Context, string, int, int) ([]*models.DimensionOption, int, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return nil, 0, errs.ErrDimensionNodeNotFound
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrDimensionNodeNotFound.Error())
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
		convey.So(mockedDataStore.GetDimensionsFromInstanceCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.GetDimensionsFromInstanceCalls()[0].ID, convey.ShouldEqual, "123")
		validateLock(mockedDataStore, "123")
		convey.So(*isLocked, convey.ShouldBeFalse)
	})
}

func TestGetDimensionsReturnsConflict(t *testing.T) {
	t.Parallel()
	convey.Convey("Get dimensions returns conflict", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		r.Header.Set("If-Match", "wrong")
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return nil, errs.ErrInstanceConflict
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusConflict)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInstanceConflict.Error())
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
		convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, "wrong")
		validateLock(mockedDataStore, "123")
		convey.So(*isLocked, convey.ShouldBeFalse)
	})
}

func TestGetDimensionsAndOptionsReturnsInternalError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return nil, errs.ErrInternalServer
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
		validateLock(mockedDataStore, "123")
		convey.So(*isLocked, convey.ShouldBeFalse)
	})

	convey.Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return &models.Instance{State: "gobbly gook"}, nil
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
		validateLock(mockedDataStore, "123")
		convey.So(*isLocked, convey.ShouldBeFalse)
	})
}

func TestGetUniqueDimensionAndOptionsReturnsOk(t *testing.T) {
	t.Parallel()

	convey.Convey("Given a dataset API with a successful store mock and auth", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetUniqueDimensionAndOptionsFunc = func(context.Context, string, string) ([]*string, int, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return []*string{}, 0, nil
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		convey.Convey("When a GET request to retrieve an instance resource is made, with a valid If-Match header", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called", func() {
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.GetUniqueDimensionAndOptionsCalls(), convey.ShouldHaveLength, 1)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When a GET request to retrieve an instance resource is made, without an If-Match header", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called, with the '*' wildchar when validting the provided If-Match value", func() {
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(mockedDataStore.GetUniqueDimensionAndOptionsCalls(), convey.ShouldHaveLength, 1)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func TestGetUniqueDimensionAndOptionsReturnsNotFound(t *testing.T) {
	t.Parallel()
	convey.Convey("Get all unique dimensions returns not found", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetUniqueDimensionAndOptionsFunc = func(context.Context, string, string) ([]*string, int, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return nil, 0, errs.ErrInstanceNotFound
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInstanceNotFound.Error())
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
		convey.So(mockedDataStore.GetUniqueDimensionAndOptionsCalls(), convey.ShouldHaveLength, 1)
		validateLock(mockedDataStore, "123")
		convey.So(*isLocked, convey.ShouldBeFalse)
	})
}

func TestGetUniqueDimensionAndOptionsReturnsConflict(t *testing.T) {
	t.Parallel()
	convey.Convey("Get all unique dimensions returns conflict", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		r.Header.Set("If-Match", "wrong")
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			return nil, errs.ErrInstanceConflict
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusConflict)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInstanceConflict.Error())
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
		convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
		convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, "wrong")
		validateLock(mockedDataStore, "123")
		convey.So(*isLocked, convey.ShouldBeFalse)
	})
}

func TestGetUniqueDimensionAndOptionsReturnsInternalError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given an internal error is returned from mongo, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return nil, errs.ErrInternalServer
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
		validateLock(mockedDataStore, "123")
		convey.So(*isLocked, convey.ShouldBeFalse)
	})

	convey.Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
		convey.So(err, convey.ShouldBeNil)

		w := httptest.NewRecorder()
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return &models.Instance{State: "gobbly gook"}, nil
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
		convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
		convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
		validateLock(mockedDataStore, "123")
		convey.So(*isLocked, convey.ShouldBeFalse)
	})
}

func TestPatchDimensions(t *testing.T) {
	t.Parallel()

	convey.Convey("Given a Dataset API instance with a mocked data store", t, func() {
		w := httptest.NewRecorder()

		numUpdateCall := 0

		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			newETag := fmt.Sprintf("%s_%d", testETag, numUpdateCall)
			numUpdateCall++
			return newETag, nil
		}
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			newETag := fmt.Sprintf("%s_%d", testETag, numUpdateCall)
			numUpdateCall++
			return newETag, nil
		}
		mockedDataStore.UpsertDimensionsToInstanceFunc = func(context.Context, []*models.CachedDimensionOption) error {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return nil
		}
		mockedDataStore.UpdateDimensionsNodeIDAndOrderFunc = func(context.Context, []*models.DimensionOption) error {
			return nil
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		convey.Convey("When calling patch dimension with a valid single patch 'upsert' operation", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/-", "value": [{"option": "op1", "dimension": "TestDim"},{"option": "op2", "dimension": "TestDim"}]}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions", body)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response is 200 OK, with the expected ETag (updated once)", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				expectedETag := fmt.Sprintf("%s_0", testETag)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, expectedETag)
			})

			convey.Convey("Then the expected database calls are performed to upsert the dimension optins in a single transaction", func() {
				validateDimensionsUpserted(mockedDataStore, "123", [][]*models.CachedDimensionOption{
					{ // single call
						{
							Option:     "op1",
							Name:       "TestDim",
							InstanceID: "123",
						},
						{
							Option:     "op2",
							Name:       "TestDim",
							InstanceID: "123",
						},
					},
				}, []string{testIfMatch})
			})

			convey.Convey("Then the db lock is acquired and released as expected, only once", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When calling patch dimension with a valid array of multiple patch 'upsert' operations", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/-", "value": [{"option": "op1", "dimension": "TestDim"}]},
				{"op": "add", "path": "/-", "value": [{"option": "op2", "dimension": "TestDim"}]}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions", body)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response is 200 OK, with the expected ETag (updated once)", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				expectedETag := fmt.Sprintf("%s_0", testETag)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, expectedETag)
			})

			convey.Convey("Then the expected database calls are performed to update node_id", func() {
				validateDimensionsUpserted(mockedDataStore, "123", [][]*models.CachedDimensionOption{
					{ // single call
						{
							Option:     "op1",
							Name:       "TestDim",
							InstanceID: "123",
						},
						{
							Option:     "op2",
							Name:       "TestDim",
							InstanceID: "123",
						},
					},
				}, []string{testIfMatch})
			})

			convey.Convey("Then the db lock is acquired and released as expected, only once", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When calling patch dimension with a valid array of multiple patch 'update' operations", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/dim1/options/op1/node_id", "value": "testNode"},
				{"op": "add", "path": "/dim2/options/op2/order", "value": 7}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions", body)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response is 200 OK, with the expected ETag (updated once)", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				expectedETag := fmt.Sprintf("%s_0", testETag)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, expectedETag)
			})

			convey.Convey("Then the expected database calls are performed to update node_id and order", func() {
				ord := 7
				validateDimensionUpdates(mockedDataStore, []*models.DimensionOption{
					{
						InstanceID: "123",
						Name:       "dim1",
						NodeID:     "testNode",
						Option:     "op1",
					},
					{
						InstanceID: "123",
						Name:       "dim2",
						Option:     "op2",
						Order:      &ord,
					},
				}, testIfMatch)
			})

			convey.Convey("Then the db lock is acquired and released as expected, only once", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func TestPatchDimensionsReturnsBadRequest(t *testing.T) {
	t.Parallel()

	convey.Convey("Given a Dataset API instance with a mocked datastore GetInstance", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(false)
		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{})

		bodies := map[string]io.Reader{
			"Then patch dimensions with an invalid body returns bad request": strings.NewReader(`wrong`),
			"Then patch dimensions with a patch containing an unsupported method returns bad request": strings.NewReader(`[{
				"op": "remove",
				"path": "/-"
			}]`),
			"Then patch dimensions with an unexpected path returns bad request": strings.NewReader(`[{
				"op": "add",
				"path": "unexpected",
				"value": [{"option": "op1", "dimension": "TestDim"},{"option": "op2", "dimension": "TestDim"}]
			}]`),
			"Then patch dimensions with an unexpected value type for path '/-' returns bad request": strings.NewReader(`[{
				"op": "add",
				"path": "/-",
				"value": {"option": "op1", "dimension": "TestDim"}
			}]`),
			"Then patch dimensions with an unexpected value type for path '/{dimension}/options/{option}/node_id' returns bad request": strings.NewReader(`[{
				"op": "add",
				"path": "/dim1/options/op1/node_id",
				"value": 8
			}]`),
			"Then patch dimensions with an unexpected value type for path '/{dimension}/options/{option}/order' returns bad request": strings.NewReader(`[{
				"op": "add",
				"path": "/dim1/options/op1/order",
				"value": "wrong"
			}]`),
			"Then patch dimensions with an option with missing parameters returns bad request": strings.NewReader(`[{
				"op": "add",
				"path": "/-",
				"value": [{"option": "op1"},{"option": "op2", "dimension": "TestDim"}]
			}]`),
			"Then patch dimensions with a total number of values greater than MaxRequestOptions in a single patch op returns bad request": strings.NewReader(`[{
				"op": "add",
				"path": "/-",
				"value": [
					{"option": "op01", "dimension": "TestDim"},
					{"option": "op02", "dimension": "TestDim"},
					{"option": "op03", "dimension": "TestDim"},
					{"option": "op04", "dimension": "TestDim"},
					{"option": "op05", "dimension": "TestDim"},
					{"option": "op06", "dimension": "TestDim"},
					{"option": "op07", "dimension": "TestDim"},
					{"option": "op08", "dimension": "TestDim"},
					{"option": "op09", "dimension": "TestDim"},
					{"option": "op10", "dimension": "TestDim"},
					{"option": "op11", "dimension": "TestDim"}
				]
			}]`),
			"Then patch dimensions with a total number of values greater than MaxRequestOptions distributed in multiple patch ops returns bad request": strings.NewReader(`[
				{
					"op": "add",
					"path": "/-",
					"value": [
						{"option": "op01", "dimension": "TestDim"},
						{"option": "op02", "dimension": "TestDim"},
						{"option": "op03", "dimension": "TestDim"},
						{"option": "op04", "dimension": "TestDim"},
						{"option": "op05", "dimension": "TestDim"},
						{"option": "op06", "dimension": "TestDim"},
						{"option": "op07", "dimension": "TestDim"},
						{"option": "op08", "dimension": "TestDim"},
						{"option": "op09", "dimension": "TestDim"}
					]
				},
				{
					"op": "add",
					"path": "/TestDim/options/op1/order",
					"value": 10
				},
				{
					"op": "add",
					"path": "/TestDim/options/op1/node_id",
					"value": "testNodeID"
				},
				{
					"op": "add",
					"path": "/-",
					"value": [
						{"option": "op12", "dimension": "TestDim"}
					]
				}
			]`),
		}

		for msg, body := range bodies {
			convey.Convey(msg, func() {
				r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions", body)
				convey.So(err, convey.ShouldBeNil)

				datasetAPI.Router.ServeHTTP(w, r)
				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		}
	})
}

func getAPIWithCMDMocks(ctx context.Context, mockedDataStore store.Storer, mockedGeneratedDownloads api.DownloadsGenerator) *api.DatasetAPI {
	downloadGenerators := map[models.DatasetType]api.DownloadsGenerator{
		models.Filterable: mockedGeneratedDownloads,
	}
	mu.Lock()
	defer mu.Unlock()

	cfg, err := config.Get()
	convey.So(err, convey.ShouldBeNil)
	cfg.ServiceAuthToken = "dataset"
	cfg.DatasetAPIURL = "http://localhost:22000"
	cfg.EnablePrivateEndpoints = true
	cfg.MaxRequestOptions = testMaxRequestOptions

	datasetPermissions := getAuthorisationHandlerMock()
	permissions := getAuthorisationHandlerMock()

	return api.Setup(ctx, cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, downloadGenerators, datasetPermissions, permissions)
}

func getAuthorisationHandlerMock() *mocks.AuthHandlerMock {
	return &mocks.AuthHandlerMock{
		Required: &mocks.PermissionCheckCalls{Calls: 0},
	}
}
