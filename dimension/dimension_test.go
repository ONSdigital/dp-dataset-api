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

	clientsidentity "github.com/ONSdigital/dp-api-clients-go/v2/identity"
	"github.com/ONSdigital/dp-authorisation/v2/authorisation"
	authMock "github.com/ONSdigital/dp-authorisation/v2/authorisation/mock"
	cloudflareMocks "github.com/ONSdigital/dp-dataset-api/cloudflare/mocks"
	permissionsAPISDK "github.com/ONSdigital/dp-permissions-api/sdk"

	"github.com/ONSdigital/dp-dataset-api/api"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/application"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/url"
	dprequest "github.com/ONSdigital/dp-net/v3/request"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	codeListAPIURL        = &neturl.URL{Scheme: "http", Host: "localhost:22400"}
	datasetAPIURL         = &neturl.URL{Scheme: "http", Host: "localhost:22000"}
	downloadServiceURL    = &neturl.URL{Scheme: "http", Host: "localhost:23600"}
	importAPIURL          = &neturl.URL{Scheme: "http", Host: "localhost:21800"}
	websiteURL            = &neturl.URL{Scheme: "http", Host: "localhost:20000"}
	apiRouterPublicURL    = &neturl.URL{Scheme: "http", Host: "localhost:23200", Path: "v1"}
	urlBuilder            = url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL, apiRouterPublicURL)
	enableURLRewriting    = false
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

func createRequestWithNoToken(method, requestURL string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequest(method, requestURL, body)
	ctx := r.Context()
	r = r.WithContext(ctx)
	return r, err
}

func validateDimensionUpdates(mockedDataStore *storetest.StorerMock, expected []*models.DimensionOption, expectedIfMatch string) {
	// Gets called twice as there is a check wrapper around this route which
	// checks the instance is not published before entering handler
	So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 2)
	So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, expected[0].InstanceID)
	So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, expectedIfMatch)
	So(mockedDataStore.GetInstanceCalls()[1].ID, ShouldEqual, expected[0].InstanceID)
	So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, ShouldEqual, expectedIfMatch)
	validateLock(mockedDataStore, expected[0].InstanceID)

	So(mockedDataStore.UpdateETagForOptionsCalls(), ShouldHaveLength, 1)
	So(mockedDataStore.UpdateETagForOptionsCalls()[0].CurrentInstance.InstanceID, ShouldEqual, expected[0].InstanceID)
	So(mockedDataStore.UpdateETagForOptionsCalls()[0].ETagSelector, ShouldEqual, expectedIfMatch)

	So(mockedDataStore.UpdateDimensionsNodeIDAndOrderCalls(), ShouldHaveLength, 1)
	So(mockedDataStore.UpdateDimensionsNodeIDAndOrderCalls()[0].Updates, ShouldResemble, expected)
}

// validateDimensionsUpserted validates that the provided expectedUpserts were performed sequentially in the provided order
func validateDimensionsUpserted(mockedDataStore *storetest.StorerMock, instanceID string, expectedUpserts [][]*models.CachedDimensionOption, expectedIfMatch []string) {
	So(len(expectedUpserts), ShouldEqual, len(expectedIfMatch))

	// Gets called is called an extra time, as there is a check wrapper around this route which
	// checks the instance is not published before entering handler
	So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, len(expectedUpserts)+1)
	So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, instanceID)
	So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, expectedIfMatch[0])
	for i := range expectedUpserts {
		So(mockedDataStore.GetInstanceCalls()[i+1].ID, ShouldEqual, instanceID)
		So(mockedDataStore.GetInstanceCalls()[i+1].ETagSelector, ShouldEqual, expectedIfMatch[i])
	}

	// validate UpdateETag calls
	So(mockedDataStore.UpdateETagForOptionsCalls(), ShouldHaveLength, len(expectedUpserts))
	for i := range expectedUpserts {
		So(mockedDataStore.UpdateETagForOptionsCalls()[i].CurrentInstance.InstanceID, ShouldEqual, instanceID)
		So(mockedDataStore.UpdateETagForOptionsCalls()[i].ETagSelector, ShouldEqual, expectedIfMatch[i])
	}

	// validate Upsert calls
	So(mockedDataStore.UpsertDimensionsToInstanceCalls(), ShouldHaveLength, len(expectedUpserts))
	for i, expected := range expectedUpserts {
		So(mockedDataStore.UpsertDimensionsToInstanceCalls()[i].Dimensions, ShouldResemble, expected)
	}
}

func validateLock(mockedDataStore *storetest.StorerMock, expectedInstanceID string) {
	So(mockedDataStore.AcquireInstanceLockCalls(), ShouldHaveLength, 1)
	So(mockedDataStore.AcquireInstanceLockCalls()[0].InstanceID, ShouldEqual, expectedInstanceID)
	So(mockedDataStore.UnlockInstanceCalls(), ShouldHaveLength, 1)
	So(mockedDataStore.UnlockInstanceCalls()[0].LockID, ShouldEqual, testLockID)
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
	}
	return storerTestMock, &isLocked
}

// Deprecated
func TestAddNodeIDToDimensionReturnsOK(t *testing.T) {
	t.Parallel()

	Convey("Given a dataset API with a successful store mock and auth", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.UpdateDimensionsNodeIDAndOrderFunc = func(context.Context, []*models.DimensionOption) error {
			So(*isLocked, ShouldBeTrue)
			return nil
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

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
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			newETag := fmt.Sprintf("%s_%d", testETag, numUpdateCall)
			numUpdateCall++
			return newETag, nil
		}
		mockedDataStore.UpdateDimensionsNodeIDAndOrderFunc = func(context.Context, []*models.DimensionOption) error {
			So(*isLocked, ShouldBeTrue)
			return nil
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

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
				validateDimensionUpdates(mockedDataStore, []*models.DimensionOption{
					{
						InstanceID: "123",
						Name:       "age",
						NodeID:     "11",
						Option:     "55",
					},
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
				validateDimensionUpdates(mockedDataStore, []*models.DimensionOption{
					{
						InstanceID: "123",
						Name:       "age",
						NodeID:     "11",
						Option:     "55",
					},
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
				validateDimensionUpdates(mockedDataStore, []*models.DimensionOption{
					{
						InstanceID: "123",
						Name:       "age",
						Option:     "55",
						Order:      &expectedOrder,
					},
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
				So(mockedDataStore.UpdateDimensionsNodeIDAndOrderCalls(), ShouldHaveLength, 2)
				So(mockedDataStore.UpdateDimensionsNodeIDAndOrderCalls()[0].Updates[0], ShouldResemble, &models.DimensionOption{
					InstanceID: "123",
					Name:       "age",
					Option:     "55",
					Order:      &expectedOrder,
				})
				So(mockedDataStore.UpdateDimensionsNodeIDAndOrderCalls()[1].Updates[0], ShouldResemble, &models.DimensionOption{
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
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.UpdateDimensionsNodeIDAndOrderFunc = func(context.Context, []*models.DimensionOption) error {
			So(*isLocked, ShouldBeTrue)
			return errs.ErrDimensionNodeNotFound
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

		Convey("Add node id to a dimension returns status not found", func() {
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)
			So(w.Code, ShouldEqual, http.StatusNotFound)

			Convey("And the expected database calls are performed to update nodeID", func() {
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
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.UpdateDimensionsNodeIDAndOrderFunc = func(context.Context, []*models.DimensionOption) error {
			So(*isLocked, ShouldBeTrue)
			return errs.ErrDimensionNodeNotFound
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

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
		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

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
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return nil, errs.ErrInternalServer
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
	})

	Convey("Given instance state is invalid, then response returns an internal error", t, func() {
		r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return &models.Instance{State: "gobbledygook"}, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
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
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			return nil, errs.ErrInternalServer
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
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
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			return &models.Instance{State: "gobbledygook"}, nil
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
		So(*isLocked, ShouldBeFalse)
	})

	Convey("Given an internal error is returned from mongo GetInstance on the second call, then response returns an internal error", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			if len(mockedDataStore.GetInstanceCalls()) == 1 {
				return &models.Instance{State: models.CreatedState}, nil
			}
			return nil, errs.ErrInternalServer
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions/age/options/55", body)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
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
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
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
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
	})
}

// Deprecated
func TestAddNodeIDToDimensionReturnsUnauthorized(t *testing.T) {
	t.Parallel()
	Convey("Add node id to a dimension of an instance returns unauthorized", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", http.NoBody)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
	})
}

func TestAddNodeIDToDimensionReturnsAuthForbidden(t *testing.T) {
	t.Parallel()
	Convey("Add node id to a dimension of an instance returns forbidden", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", http.NoBody)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		Convey("Then none of the expected functions are called", func() {
			So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpdateETagForOptionsCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpsertDimensionsToInstanceCalls(), ShouldHaveLength, 0)
		})
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		Convey("Then none of the expected functions are called", func() {
			So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpdateETagForOptionsCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpsertDimensionsToInstanceCalls(), ShouldHaveLength, 0)
		})
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
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.UpsertDimensionsToInstanceFunc = func(context.Context, []*models.CachedDimensionOption) error {
			So(*isLocked, ShouldBeTrue)
			return nil
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

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
				So(mockedDataStore.UpdateETagForOptionsCalls()[0].Upserts[0], ShouldResemble, expected)
				So(mockedDataStore.UpsertDimensionsToInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.UpsertDimensionsToInstanceCalls()[0].Dimensions[0], ShouldResemble, expected)
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
				So(mockedDataStore.UpdateETagForOptionsCalls()[0].Upserts[0], ShouldResemble, expected)
				So(mockedDataStore.UpsertDimensionsToInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.UpsertDimensionsToInstanceCalls()[0].Dimensions[0], ShouldResemble, expected)
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
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			return testETag, nil
		}
		mockedDataStore.UpsertDimensionsToInstanceFunc = func(context.Context, []*models.CachedDimensionOption) error {
			So(*isLocked, ShouldBeTrue)
			return errs.ErrDimensionNodeNotFound
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
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
		So(mockedDataStore.UpdateETagForOptionsCalls()[0].Upserts[0], ShouldResemble, expected)

		So(mockedDataStore.UpsertDimensionsToInstanceCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.UpsertDimensionsToInstanceCalls()[0].Dimensions[0], ShouldResemble, expected)
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
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return &models.Instance{State: models.PublishedState}, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
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

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusUnauthorized)
		Convey("Then none of the expected functions are called", func() {
			So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpdateETagForOptionsCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpsertDimensionsToInstanceCalls(), ShouldHaveLength, 0)
		})
	})
}

func TestAddDimensionToInstanceAuthForbidden(t *testing.T) {
	t.Parallel()
	Convey("Add a dimension to a instance returns forbidden", t, func() {
		json := strings.NewReader(`{"value":"24", "code_list":"123-456", "dimension": "test"}`)
		r, err := http.NewRequest("POST", "http://localhost:21800/instances/123/dimensions", json)
		So(err, ShouldBeNil)

		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusForbidden)
		Convey("Then none of the expected functions are called", func() {
			So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpdateETagForOptionsCalls(), ShouldHaveLength, 0)
			So(mockedDataStore.UpsertDimensionsToInstanceCalls(), ShouldHaveLength, 0)
		})
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
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return nil, errs.ErrInternalServer
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
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
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return &models.Instance{State: "gobbledygook"}, nil
			},
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		// Gets called twice as there is a check wrapper around this route which
		// checks the instance is not published before entering handler
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
	})
}

func TestGetDimensionsUnauthorised(t *testing.T) {
	t.Parallel()

	Convey("Given a dataset API where the authorisation token is missing", t, func() {
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

		Convey("When a GET request to retrieve an instance resource is made, with a valid If-Match header", func() {
			r, err := createRequestWithNoToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 401 unauthorized, with no ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusUnauthorized)
			})

			Convey("Then the expected functions are not called", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 0)
				So(mockedDataStore.GetDimensionsFromInstanceCalls(), ShouldHaveLength, 0)
			})
		})
	})
}

func TestGetDimensionsForbidden(t *testing.T) {
	t.Parallel()

	Convey("Given a dataset API request where the authorisation is forbidden", t, func() {
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

		Convey("When a GET request to retrieve an instance resource is made, with a valid If-Match header", func() {
			r, err := createRequestWithNoToken("GET", "http://localhost:21800/instances/123/dimensions", nil)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 403 forbidden, with no ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusForbidden)
			})

			Convey("Then the expected functions are not called", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 0)
				So(mockedDataStore.GetDimensionsFromInstanceCalls(), ShouldHaveLength, 0)
			})
		})
	})
}

func TestGetDimensionsReturnsOk(t *testing.T) {
	t.Parallel()

	Convey("Given a dataset API with a successful store mock and auth", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetDimensionsFromInstanceFunc = func(context.Context, string, int, int) ([]*models.DimensionOption, int, error) {
			So(*isLocked, ShouldBeTrue)
			return []*models.DimensionOption{}, 0, nil
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

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
		mockedDataStore.GetDimensionsFromInstanceFunc = func(context.Context, string, int, int) ([]*models.DimensionOption, int, error) {
			So(*isLocked, ShouldBeTrue)
			return nil, 0, errs.ErrDimensionNodeNotFound
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
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
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			So(*isLocked, ShouldBeTrue)
			return nil, errs.ErrInstanceConflict
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
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
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			So(*isLocked, ShouldBeTrue)
			return nil, errs.ErrInternalServer
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
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
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			So(*isLocked, ShouldBeTrue)
			return &models.Instance{State: "gobbly gook"}, nil
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
		So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
		validateLock(mockedDataStore, "123")
		So(*isLocked, ShouldBeFalse)
	})
}

func TestGetUniqueDimensionAndOptionsUnauthorised(t *testing.T) {
	t.Parallel()

	Convey("Given a dataset API with an auth response that is unauthorized", t, func() {
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

		Convey("When a GET request to retrieve an dimension options is made, with a valid If-Match header", func() {
			r, err := createRequestWithNoToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 401 unauthorized", func() {
				So(w.Code, ShouldEqual, http.StatusUnauthorized)
			})

			Convey("Then none of the expected functions are called", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 0)
				So(mockedDataStore.GetUniqueDimensionAndOptionsCalls(), ShouldHaveLength, 0)
			})
		})
	})
}

func TestGetUniqueDimensionAndOptionsForbidden(t *testing.T) {
	t.Parallel()

	Convey("Given a dataset API with an auth response that is forbidden", t, func() {
		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

		Convey("When a GET request to retrieve an dimension options is made, with a valid If-Match header", func() {
			r, err := createRequestWithNoToken("GET", "http://localhost:21800/instances/123/dimensions/age/options", nil)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 403 forbidden", func() {
				So(w.Code, ShouldEqual, http.StatusForbidden)
			})

			Convey("Then none of the expected functions are called", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 0)
				So(mockedDataStore.GetUniqueDimensionAndOptionsCalls(), ShouldHaveLength, 0)
			})
		})
	})
}

func TestGetUniqueDimensionAndOptionsReturnsOk(t *testing.T) {
	t.Parallel()

	Convey("Given a dataset API with a successful store mock and auth", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(false)
		mockedDataStore.GetUniqueDimensionAndOptionsFunc = func(context.Context, string, string) ([]*string, int, error) {
			So(*isLocked, ShouldBeTrue)
			return []*string{}, 0, nil
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

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
		mockedDataStore.GetUniqueDimensionAndOptionsFunc = func(context.Context, string, string) ([]*string, int, error) {
			So(*isLocked, ShouldBeTrue)
			return nil, 0, errs.ErrInstanceNotFound
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
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
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			return nil, errs.ErrInstanceConflict
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
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
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			So(*isLocked, ShouldBeTrue)
			return nil, errs.ErrInternalServer
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
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
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			So(*isLocked, ShouldBeTrue)
			return &models.Instance{State: "gobbly gook"}, nil
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})
		datasetAPI.Router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
		So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
		validateLock(mockedDataStore, "123")
		So(*isLocked, ShouldBeFalse)
	})
}

func TestPatchDimensionsUnauthorised(t *testing.T) {
	t.Parallel()

	Convey("Given a Dataset API instance with a mocked data store", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

		Convey("When calling patch dimension with no authorisation information supplied", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/-", "value": [{"option": "op1", "dimension": "TestDim"},{"option": "op2", "dimension": "TestDim"}]}
			]`)
			r, err := createRequestWithNoToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response is 401 unauthorized", func() {
				So(w.Code, ShouldEqual, http.StatusUnauthorized)
			})

			Convey("Then the none of the expected database calls are performed", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 0)
				So(mockedDataStore.UpdateETagForOptionsCalls(), ShouldHaveLength, 0)
				So(mockedDataStore.UpsertDimensionsToInstanceCalls(), ShouldHaveLength, 0)
			})
		})
	})
}

func TestPatchDimensionsForbidden(t *testing.T) {
	t.Parallel()

	Convey("Given a Dataset API instance with a mocked data store, where the auth returns forbidden", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

		Convey("When calling patch dimension witn a invalid auth", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/-", "value": [{"option": "op1", "dimension": "TestDim"},{"option": "op2", "dimension": "TestDim"}]}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response is 403 forbidden", func() {
				So(w.Code, ShouldEqual, http.StatusForbidden)
			})

			Convey("Then the none of the expected database calls are performed", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 0)
				So(mockedDataStore.UpdateETagForOptionsCalls(), ShouldHaveLength, 0)
				So(mockedDataStore.UpsertDimensionsToInstanceCalls(), ShouldHaveLength, 0)
			})
		})
	})
}

func TestPatchDimensions(t *testing.T) {
	t.Parallel()

	Convey("Given a Dataset API instance with a mocked data store", t, func() {
		w := httptest.NewRecorder()

		numUpdateCall := 0

		mockedDataStore, isLocked := storeMockWithLock(true)
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			newETag := fmt.Sprintf("%s_%d", testETag, numUpdateCall)
			numUpdateCall++
			return newETag, nil
		}
		mockedDataStore.UpdateETagForOptionsFunc = func(context.Context, *models.Instance, []*models.CachedDimensionOption, []*models.DimensionOption, string) (string, error) {
			So(*isLocked, ShouldBeTrue)
			newETag := fmt.Sprintf("%s_%d", testETag, numUpdateCall)
			numUpdateCall++
			return newETag, nil
		}
		mockedDataStore.UpsertDimensionsToInstanceFunc = func(context.Context, []*models.CachedDimensionOption) error {
			So(*isLocked, ShouldBeTrue)
			return nil
		}
		mockedDataStore.UpdateDimensionsNodeIDAndOrderFunc = func(context.Context, []*models.DimensionOption) error {
			return nil
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

		Convey("When calling patch dimension with a valid single patch 'upsert' operation", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/-", "value": [{"option": "op1", "dimension": "TestDim"},{"option": "op2", "dimension": "TestDim"}]}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response is 200 OK, with the expected ETag (updated once)", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				expectedETag := fmt.Sprintf("%s_0", testETag)
				So(w.Header().Get("ETag"), ShouldEqual, expectedETag)
			})

			Convey("Then the expected database calls are performed to upsert the dimension optins in a single transaction", func() {
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

			Convey("Then the db lock is acquired and released as expected, only once", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When calling patch dimension with a valid array of multiple patch 'upsert' operations", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/-", "value": [{"option": "op1", "dimension": "TestDim"}]},
				{"op": "add", "path": "/-", "value": [{"option": "op2", "dimension": "TestDim"}]}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response is 200 OK, with the expected ETag (updated once)", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				expectedETag := fmt.Sprintf("%s_0", testETag)
				So(w.Header().Get("ETag"), ShouldEqual, expectedETag)
			})

			Convey("Then the expected database calls are performed to update node_id", func() {
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

			Convey("Then the db lock is acquired and released as expected, only once", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When calling patch dimension with a valid array of multiple patch 'update' operations", func() {
			body := strings.NewReader(`[
				{"op": "add", "path": "/dim1/options/op1/node_id", "value": "testNode"},
				{"op": "add", "path": "/dim2/options/op2/order", "value": 7}
			]`)
			r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)

			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response is 200 OK, with the expected ETag (updated once)", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				expectedETag := fmt.Sprintf("%s_0", testETag)
				So(w.Header().Get("ETag"), ShouldEqual, expectedETag)
			})

			Convey("Then the expected database calls are performed to update node_id and order", func() {
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

			Convey("Then the db lock is acquired and released as expected, only once", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func TestPatchDimensionsReturnsBadRequest(t *testing.T) {
	t.Parallel()

	Convey("Given a Dataset API instance with a mocked datastore GetInstance", t, func() {
		w := httptest.NewRecorder()

		mockedDataStore, isLocked := storeMockWithLock(false)

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}
		datasetAPI := getAPIWithCMDMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock, &cloudflareMocks.ClienterMock{})

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
			Convey(msg, func() {
				r, err := createRequestWithToken(http.MethodPatch, "http://localhost:21800/instances/123/dimensions", body)
				So(err, ShouldBeNil)

				datasetAPI.Router.ServeHTTP(w, r)
				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
				So(*isLocked, ShouldBeFalse)
			})
		}
	})
}

func getAPIWithCMDMocks(ctx context.Context, mockedDataStore store.Storer, mockedGeneratedDownloads api.DownloadsGenerator, authorisationMock *authMock.MiddlewareMock, cloudflareMock *cloudflareMocks.ClienterMock) *api.DatasetAPI {
	downloadGenerators := map[models.DatasetType]api.DownloadsGenerator{
		models.Filterable: mockedGeneratedDownloads,
	}

	mockedMapSMGeneratedDownloads := map[models.DatasetType]application.DownloadsGenerator{
		models.Filterable: mockedGeneratedDownloads,
	}

	mockStatemachineDatasetAPI := application.StateMachineDatasetAPI{
		DataStore:          store.DataStore{Backend: mockedDataStore},
		DownloadGenerators: mockedMapSMGeneratedDownloads,
	}

	permissionsChecker := &authMock.PermissionsCheckerMock{
		HasPermissionFunc: func(ctx context.Context, entityData permissionsAPISDK.EntityData, permission string, attributes map[string]string) (bool, error) {
			return true, nil
		},
	}

	if authorisationMock == nil {
		authorisationMock = &authMock.MiddlewareMock{}
	}
	if authorisationMock.RequireFunc == nil {
		authorisationMock.RequireFunc = func(_ string, handlerFunc http.HandlerFunc) http.HandlerFunc {
			return handlerFunc
		}
	}
	if authorisationMock.RequireWithAttributesFunc == nil {
		authorisationMock.RequireWithAttributesFunc = func(_ string, handlerFunc http.HandlerFunc, _ authorisation.GetAttributesFromRequest) http.HandlerFunc {
			return handlerFunc
		}
	}
	mu.Lock()
	defer mu.Unlock()

	cfg, err := config.Get()
	So(err, ShouldBeNil)
	cfg.ServiceAuthToken = "dataset"
	cfg.DatasetAPIURL = "http://localhost:22000"
	cfg.EnablePrivateEndpoints = true
	cfg.MaxRequestOptions = testMaxRequestOptions

	testIdentityClient := clientsidentity.New(cfg.ZebedeeURL)

	return api.Setup(ctx, cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, downloadGenerators, authorisationMock, enableURLRewriting, &mockStatemachineDatasetAPI, permissionsChecker, testIdentityClient, nil, cloudflareMock)
}
