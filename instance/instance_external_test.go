package instance_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"strings"
	"sync"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/api"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/application"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/instance"
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
	mu                 sync.Mutex
	testContext        = context.Background()
	codeListAPIURL     = &neturl.URL{Scheme: "http", Host: "localhost:22400"}
	datasetAPIURL      = &neturl.URL{Scheme: "http", Host: "localhost:22000"}
	downloadServiceURL = &neturl.URL{Scheme: "http", Host: "localhost:23600"}
	importAPIURL       = &neturl.URL{Scheme: "http", Host: "localhost:21800"}
	websiteURL         = &neturl.URL{Scheme: "http", Host: "localhost:20000"}
	urlBuilder         = url.NewBuilder(websiteURL, downloadServiceURL, datasetAPIURL, codeListAPIURL, importAPIURL)
	enableURLRewriting = false
	enableStateMachine = false
)

func createRequestWithToken(method, requestURL string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequest(method, requestURL, body)
	ctx := r.Context()
	ctx = dprequest.SetCaller(ctx, "someone@ons.gov.uk")
	r = r.WithContext(ctx)
	return r, err
}

func initAPIWithMockedStore(mockedStore *storetest.StorerMock) *instance.Store {
	instanceAPI := &instance.Store{
		Storer:     mockedStore,
		URLBuilder: urlBuilder,
	}
	return instanceAPI
}

func Test_GetInstancesReturnsOK(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a GET request to retrieve a list of instance resources is made", t, func() {
		convey.Convey("Then return status ok (200)", func() {
			r := httptest.NewRequest("GET", "http://foo/instances", http.NoBody)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstancesFunc: func(context.Context, []string, []string, int, int) ([]*models.Instance, int, error) {
					return []*models.Instance{}, 0, nil
				},
			}

			instanceAPI := initAPIWithMockedStore(mockedDataStore)
			list, totalCount, err := instanceAPI.GetList(w, r, 20, 0)

			convey.So(len(mockedDataStore.GetInstancesCalls()), convey.ShouldEqual, 1)
			convey.So(totalCount, convey.ShouldEqual, 0)
			convey.So(list, convey.ShouldResemble, []*models.Instance{})
			convey.So(err, convey.ShouldEqual, nil)
		})

		convey.Convey("When the request includes a filter by state of 'completed' this is delegated to the database function", func() {
			r := httptest.NewRequest("GET", "http://foo/instances?state=completed", http.NoBody)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstancesFunc: func(context.Context, []string, []string, int, int) ([]*models.Instance, int, error) {
					return []*models.Instance{{InstanceID: "test"}}, 1, nil
				},
			}

			instanceAPI := initAPIWithMockedStore(mockedDataStore)
			list, totalCount, err := instanceAPI.GetList(w, r, 20, 0)

			convey.So(len(mockedDataStore.GetInstancesCalls()), convey.ShouldEqual, 1)
			convey.So(mockedDataStore.GetInstancesCalls()[0].States, convey.ShouldResemble, []string{"completed"})
			convey.So(totalCount, convey.ShouldEqual, 1)
			convey.So(list, convey.ShouldResemble, []*models.Instance{{InstanceID: "test"}})
			convey.So(err, convey.ShouldEqual, nil)
		})

		convey.Convey("When the request includes a filter by dataset of 'test' this is delegated to the database function", func() {
			r := httptest.NewRequest("GET", "http://foo/instances?dataset=test", http.NoBody)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstancesFunc: func(context.Context, []string, []string, int, int) ([]*models.Instance, int, error) {
					return []*models.Instance{}, 0, nil
				},
			}

			instanceAPI := initAPIWithMockedStore(mockedDataStore)
			_, _, _ = instanceAPI.GetList(w, r, 20, 0)

			convey.So(mockedDataStore.GetInstancesCalls()[0].Datasets, convey.ShouldResemble, []string{"test"})
			convey.So(len(mockedDataStore.GetInstancesCalls()), convey.ShouldEqual, 1)
		})

		convey.Convey("When the request includes a filter by state of multiple values 'completed,edition-confirmed' these are all delegated to the database function", func() {
			r := httptest.NewRequest("GET", "http://foo/instances?state=completed,edition-confirmed", http.NoBody)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstancesFunc: func(context.Context, []string, []string, int, int) ([]*models.Instance, int, error) {
					return []*models.Instance{}, 0, nil
				},
			}

			instanceAPI := initAPIWithMockedStore(mockedDataStore)
			_, _, _ = instanceAPI.GetList(w, r, 20, 0)

			convey.So(mockedDataStore.GetInstancesCalls()[0].States, convey.ShouldResemble, []string{"completed", "edition-confirmed"})
			convey.So(len(mockedDataStore.GetInstancesCalls()), convey.ShouldEqual, 1)
		})

		convey.Convey("When the request includes a filter by state of 'completed' and dataset 'test'", func() {
			r := httptest.NewRequest("GET", "http://foo/instances?state=completed&dataset=test", http.NoBody)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				GetInstancesFunc: func(context.Context, []string, []string, int, int) ([]*models.Instance, int, error) {
					return []*models.Instance{}, 0, nil
				},
			}

			instanceAPI := initAPIWithMockedStore(mockedDataStore)
			_, _, _ = instanceAPI.GetList(w, r, 20, 0)

			convey.So(mockedDataStore.GetInstancesCalls()[0].States, convey.ShouldResemble, []string{"completed"})
			convey.So(mockedDataStore.GetInstancesCalls()[0].Datasets, convey.ShouldResemble, []string{"test"})
			convey.So(len(mockedDataStore.GetInstancesCalls()), convey.ShouldEqual, 1)
		})
	})
}

func Test_GetInstancesReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a GET request to retrieve a list of instance resources is made", t, func() {
		convey.Convey("When the service is unable to connect to the datastore", func() {
			convey.Convey("Then return status internal server error (500)", func() {
				r := httptest.NewRequest("GET", "http://localhost:21800/instances", http.NoBody)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstancesFunc: func(context.Context, []string, []string, int, int) ([]*models.Instance, int, error) {
						return nil, 0, errs.ErrInternalServer
					},
				}

				instanceAPI := initAPIWithMockedStore(mockedDataStore)
				_, _, _ = instanceAPI.GetList(w, r, 20, 0)

				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
				convey.So(len(mockedDataStore.GetInstancesCalls()), convey.ShouldEqual, 1)
			})
		})

		convey.Convey("When the request contains an invalid state to filter on", func() {
			convey.Convey("Then return status bad request (400)", func() {
				r := httptest.NewRequest("GET", "http://foo/instances?state=foo", http.NoBody)
				w := httptest.NewRecorder()

				instanceAPI := initAPIWithMockedStore(&storetest.StorerMock{})
				_, _, _ = instanceAPI.GetList(w, r, 20, 0)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "bad request - invalid filter state values: [foo]")
			})
		})
	})
}

func Test_GetInstanceReturnsOK(t *testing.T) {
	t.Parallel()

	convey.Convey("Given a dataset API with a successful store mock and auth", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
				return &models.Instance{
					State: models.CreatedState,
					ETag:  testETag,
				}, nil
			},
		}
		datasetPermissions := mocks.NewAuthHandlerMock()
		permissions := mocks.NewAuthHandlerMock()
		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		convey.Convey("When a GET request to retrieve an instance resource is made, with a valid If-Match header", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123", http.NoBody)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()

			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called", func() {
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
			})
		})

		convey.Convey("When a GET request to retrieve an instance resource is made, without an If-Match header", func() {
			r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123", http.NoBody)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()

			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called, with the '*' wildchar when validting the provided If-Match value", func() {
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
			})
		})
	})
}

func Test_GetInstanceReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a GET request to retrieve an instance resource is made", t, func() {
		convey.Convey("When the service is unable to connect to the datastore", func() {
			convey.Convey("Then return status internal server error (500)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123", http.NoBody)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
						return nil, errs.ErrInternalServer
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
			})
		})

		convey.Convey("When the current instance state is invalid", func() {
			convey.Convey("Then return status internal server error (500)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123", http.NoBody)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
						return &models.Instance{State: "gobbledygook"}, nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
			})
		})

		convey.Convey("When the instance resource does not exist", func() {
			convey.Convey("Then return status not found (404)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123", http.NoBody)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
						return nil, errs.ErrInstanceNotFound
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInstanceNotFound.Error())
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
			})
		})

		convey.Convey("When the instance resource eTag does not match the provided If-Match header value", func() {
			convey.Convey("Then return status conflict (409)", func() {
				r, err := createRequestWithToken("GET", "http://localhost:21800/instances/123", http.NoBody)
				r.Header.Set("If-Match", "wrong")
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
						return nil, errs.ErrInstanceConflict
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusConflict)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInstanceConflict.Error())
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, "wrong")
			})
		})
	})
}

func Test_AddInstanceReturnsCreated(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a POST request to create an instance resource", t, func() {
		convey.Convey("When the request is authorised", func() {
			convey.Convey("Then return status created (201)", func() {
				body := strings.NewReader(`{"links": { "job": { "id":"123-456", "href":"http://localhost:2200/jobs/123-456" } } }`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					AddInstanceFunc: func(context.Context, *models.Instance) (*models.Instance, error) {
						return &models.Instance{
							ETag: testETag,
						}, nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusCreated)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
				convey.So(len(mockedDataStore.AddInstanceCalls()), convey.ShouldEqual, 1)

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
			})
		})
	})
}

func Test_AddInstanceReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a POST request to create an instance resources", t, func() {
		convey.Convey("When the service is unable to connect to the datastore", func() {
			convey.Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"links": {"job": { "id":"123-456", "href":"http://localhost:2200/jobs/123-456" } } }`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{
					AddInstanceFunc: func(context.Context, *models.Instance) (*models.Instance, error) {
						return nil, errs.ErrInternalServer
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.AddInstanceCalls()), convey.ShouldEqual, 1)
			})
		})

		convey.Convey("When the request contains invalid json", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					AddInstanceFunc: func(context.Context, *models.Instance) (*models.Instance, error) {
						return &models.Instance{}, nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.AddInstanceCalls()), convey.ShouldEqual, 0)
			})
		})

		convey.Convey("When the request contains empty json", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{}`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					AddInstanceFunc: func(context.Context, *models.Instance) (*models.Instance, error) {
						return &models.Instance{}, nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrMissingJobProperties.Error())
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.AddInstanceCalls()), convey.ShouldEqual, 0)
			})
		})
	})
}

func Test_UpdateInstanceReturnsOk(t *testing.T) {
	t.Parallel()

	convey.Convey("Given a dataset API with a successful store mock and auth", t, func() {
		mockedDataStore, isLocked := storeMockWithLock(&models.Instance{
			InstanceID: "123",
			Links: &models.InstanceLinks{
				Dataset: &models.LinkObject{
					ID:   "234",
					HRef: "example.com/234",
				},
				Self: &models.LinkObject{
					ID:   "123",
					HRef: "example.com/123",
				},
			},
			State: models.CreatedState,
			ETag:  testETag,
		}, true)

		mockedDataStore.UpdateInstanceFunc = func(context.Context, *models.Instance, *models.Instance, string) (string, error) {
			convey.So(*isLocked, convey.ShouldBeTrue)
			return testETag, nil
		}

		datasetPermissions := mocks.NewAuthHandlerMock()
		permissions := mocks.NewAuthHandlerMock()
		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		convey.Convey("When a PUT request to update state of an instance resource to 'submitted' is made with a valid If-Match header", func() {
			body := strings.NewReader(`{"state":"submitted"}`)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
			r.Header.Set("If-Match", testIfMatch)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called", func() {
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 3)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.GetInstanceCalls()[1].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.GetInstanceCalls()[2].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[2].ETagSelector, convey.ShouldEqual, testETag)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.UpdateInstanceCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.UpdateInstanceCalls()[0].CurrentInstance.InstanceID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.UpdateInstanceCalls()[0].UpdatedInstance.State, convey.ShouldEqual, models.SubmittedState)
			})

			convey.Convey("Then the mongoDB instance lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When a PUT request to update state of an instance resource to 'submitted' is made without an If-Match header", func() {
			body := strings.NewReader(`{"state":"submitted"}`)
			r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called", func() {
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 3)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(mockedDataStore.GetInstanceCalls()[1].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(mockedDataStore.GetInstanceCalls()[2].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[2].ETagSelector, convey.ShouldEqual, testETag)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(mockedDataStore.UpdateInstanceCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(mockedDataStore.UpdateInstanceCalls()[0].CurrentInstance.InstanceID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.UpdateInstanceCalls()[0].UpdatedInstance.State, convey.ShouldEqual, models.SubmittedState)
			})

			convey.Convey("Then the mongoDB instance lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateInstanceReturnsError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a PUT request to update state of an instance resource is made", t, func() {
		convey.Convey("When the service is unable to connect to the datastore", func() {
			convey.Convey("Then return status internal server error (500)", func() {
				body := strings.NewReader(`{"state":"created"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
						return nil, errs.ErrInternalServer
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)

				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), convey.ShouldEqual, 0)
			})
		})

		convey.Convey("When the current instance state is invalid", func() {
			convey.Convey("Then return status internal server error (500)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", strings.NewReader(`{"state":"completed", "edition": "2017"}`))
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
						return &models.Instance{State: "gobbledygook"}, nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), convey.ShouldEqual, 0)
			})
		})

		convey.Convey("When the json body is invalid", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"state":`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
						return &models.Instance{State: "completed"}, nil
					},
				}
				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)

				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), convey.ShouldEqual, 0)
			})
		})

		convey.Convey("When the json body contains fields that are not allowed to be updated", func() {
			convey.Convey("Then return status bad request (400)", func() {
				body := strings.NewReader(`{"links": { "dataset": { "href": "silly-site"}, "version": { "href": "sillier-site"}}}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
						return &models.Instance{State: "completed"}, nil
					},
					UpdateInstanceFunc: func(context.Context, *models.Instance, *models.Instance, string) (string, error) {
						return testETag, nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, "unable to update instance contains invalid fields: [instance.Links.Dataset instance.Links.Version]")

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)

				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 0)
			})
		})

		convey.Convey("When the instance does not exist", func() {
			convey.Convey("Then return status not found (404)", func() {
				body := strings.NewReader(`{"edition": "2017"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
						return nil, errs.ErrInstanceNotFound
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInstanceNotFound.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)

				convey.So(len(mockedDataStore.GetInstanceCalls()), convey.ShouldEqual, 1)
				convey.So(len(mockedDataStore.UpdateInstanceCalls()), convey.ShouldEqual, 0)
				convey.So(len(mockedDataStore.AddVersionDetailsToInstanceCalls()), convey.ShouldEqual, 0)
			})
		})

		convey.Convey("When the instance eTag does not match the provided if-Match header value", func() {
			convey.Convey("Then return status conflict (409)", func() {
				body := strings.NewReader(`{"edition": "2017"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
				r.Header.Set("If-Match", testIfMatch)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(context.Context, string, string) (*models.Instance, error) {
						return nil, errs.ErrInstanceConflict
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusConflict)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInstanceConflict.Error())

				convey.So(datasetPermissions.Required.Calls, convey.ShouldEqual, 0)
				convey.So(permissions.Required.Calls, convey.ShouldEqual, 1)

				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
			})
		})
	})
}

func getAPIWithCantabularMocks(ctx context.Context, mockedDataStore store.Storer, mockedGeneratedDownloads api.DownloadsGenerator, datasetPermissions, permissions api.AuthHandler) *api.DatasetAPI {
	mockedMapDownloadGenerators := map[models.DatasetType]api.DownloadsGenerator{
		models.Filterable: mockedGeneratedDownloads,
	}

	mockedMapSMGeneratedDownloads := map[models.DatasetType]application.DownloadsGenerator{
		models.Filterable: mockedGeneratedDownloads,
	}

	mockStatemachineDatasetAPI := application.StateMachineDatasetAPI{
		DataStore:          store.DataStore{Backend: mockedDataStore},
		DownloadGenerators: mockedMapSMGeneratedDownloads,
	}
	mu.Lock()
	defer mu.Unlock()
	cfg, err := config.Get()
	convey.So(err, convey.ShouldBeNil)
	cfg.ServiceAuthToken = "dataset"
	cfg.DatasetAPIURL = "http://localhost:22000"
	cfg.EnablePrivateEndpoints = true

	return api.Setup(ctx, cfg, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, urlBuilder, mockedMapDownloadGenerators, datasetPermissions, permissions, enableURLRewriting, &mockStatemachineDatasetAPI, enableStateMachine)
}
