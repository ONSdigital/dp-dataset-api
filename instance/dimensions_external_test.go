package instance_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	authMock "github.com/ONSdigital/dp-authorisation/v2/authorisation/mock"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testIfMatch = "testIfMatch"
	testETag    = "testETag"
	AnyETag     = "*"
)

func Test_UpdateDimensionUnauthorised(t *testing.T) {
	t.Parallel()

	bodyStr := `{"label":"ages", "description": "A range of ages between 18 and 60"}`

	Convey("Given a dataset API with a successful store mock with auth that returns unauthorised", t, func() {

		mockedDataStore := &storetest.StorerMock{}
		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}

			},
		}

		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)

		Convey("When a PUT request to update an instance dimension is made, with a valid If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithNoToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 401 unauthorized", func() {
				So(w.Code, ShouldEqual, http.StatusUnauthorized)
			})

			Convey("Then none of the expected functions are called", func() {
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
			})
		})
	})
}

func Test_UpdateDimensionForbidden(t *testing.T) {
	t.Parallel()

	bodyStr := `{"label":"ages", "description": "A range of ages between 18 and 60"}`

	Convey("Given a dataset API with a successful store mock with auth that returns forbidden", t, func() {

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}

			},
		}

		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)

		Convey("When a PUT request to update an instance dimension is made, with a valid If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 403 forbidden", func() {
				So(w.Code, ShouldEqual, http.StatusForbidden)
			})

			Convey("Then none of the expected functions are called", func() {
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
			})
		})
	})
}

func Test_UpdateDimensionReturnsOk(t *testing.T) {
	t.Parallel()

	bodyStr := `{"label":"ages", "description": "A range of ages between 18 and 60"}`
	expectedUpdate := &models.Instance{
		Dimensions: []models.Dimension{
			{
				Label:       "ages",
				Description: "A range of ages between 18 and 60",
				ID:          "age",
				Name:        "age",
			},
		},
	}

	Convey("Given a dataset API with a successful store mock and auth", t, func() {
		instance := &models.Instance{
			InstanceID: "123",
		}
		mockedDataStore, isLocked := storeMockWithLock(instance, false)
		mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
			return &models.Instance{State: models.EditionConfirmedState,
				InstanceID: "123",
				Dimensions: []models.Dimension{{Name: "age", ID: "age"}}}, nil
		}
		mockedDataStore.UpdateInstanceFunc = func(context.Context, *models.Instance, *models.Instance, string) (string, error) {
			return testETag, nil
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)

		Convey("When a PUT request to update an instance dimension is made, with a valid If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called", func() {
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.GetInstanceCalls()[1].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, ShouldEqual, testIfMatch)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
				So(mockedDataStore.UpdateInstanceCalls()[0].CurrentInstance.InstanceID, ShouldEqual, "123")
				So(mockedDataStore.UpdateInstanceCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.UpdateInstanceCalls()[0].UpdatedInstance, ShouldResemble, expectedUpdate)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When a PUT request to update an instance dimension is made, without an If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called", func() {
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, AnyETag)
				So(mockedDataStore.GetInstanceCalls()[1].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[1].ETagSelector, ShouldEqual, AnyETag)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
				So(mockedDataStore.UpdateInstanceCalls()[0].CurrentInstance.InstanceID, ShouldEqual, "123")
				So(mockedDataStore.UpdateInstanceCalls()[0].ETagSelector, ShouldEqual, AnyETag)
				So(mockedDataStore.UpdateInstanceCalls()[0].UpdatedInstance, ShouldResemble, expectedUpdate)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func Test_UpdateDimensionReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Given a PUT request to update a dimension on an instance resource", t, func() {
		Convey("When service is unable to connect to datastore", func() {
			Convey("Then return status internal error (500)", func() {
				body := strings.NewReader(`{"label":"ages"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return nil, errs.ErrInternalServer
				}
				mockedDataStore.UpdateInstanceFunc = func(context.Context, *models.Instance, *models.Instance, string) (string, error) {
					return testETag, nil
				}

				authorisationMock := &authMock.MiddlewareMock{
					RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
						return handlerFunc
					},
				}

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the current instance state is invalid", func() {
			Convey("Then return status internal error (500)", func() {
				body := strings.NewReader(`{"label":"ages"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
				r.Header.Set("If-Match", testIfMatch)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: "gobbly gook"}, nil
				}
				mockedDataStore.UpdateInstanceFunc = func(context.Context, *models.Instance, *models.Instance, string) (string, error) {
					return testETag, nil
				}

				authorisationMock := &authMock.MiddlewareMock{
					RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
						return handlerFunc
					},
				}

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the resource has a state of published", func() {
			Convey("Then return status forbidden (403)", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.PublishedState}, nil
				}

				authorisationMock := &authMock.MiddlewareMock{
					RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
						return handlerFunc
					},
				}

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusForbidden)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrResourcePublished.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the instance does not exist", func() {
			Convey("Then return status not found (404) with message 'instance not found'", func() {
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", nil)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return nil, errs.ErrInstanceNotFound
				}

				authorisationMock := &authMock.MiddlewareMock{
					RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
						return handlerFunc
					},
				}

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the dimension does not exist against instance", func() {
			Convey("Then return status not found (404) with message 'dimension not found'", func() {
				body := strings.NewReader(`{"label":"notages"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/notage", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.EditionConfirmedState,
						InstanceID: "123",
						Dimensions: []models.Dimension{{Name: "age", ID: "age"}}}, nil
				}
				mockedDataStore.UpdateInstanceFunc = func(context.Context, *models.Instance, *models.Instance, string) (string, error) {
					return testETag, nil
				}

				authorisationMock := &authMock.MiddlewareMock{
					RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
						return handlerFunc
					},
				}

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrDimensionNotFound.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the request body is invalid json", func() {
			Convey("Then return status bad request (400)", func() {
				body := strings.NewReader("{")
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return &models.Instance{State: models.CompletedState}, nil
				}

				authorisationMock := &authMock.MiddlewareMock{
					RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
						return handlerFunc
					},
				}

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 2)
				So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)

				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When the provided If-Match header value does not match the instance eTag", func() {
			Convey("Then return status conflict (409)", func() {
				body := strings.NewReader(`{"label":"ages", "description": "A range of ages between 18 and 60"}`)
				r, err := createRequestWithToken("PUT", "http://localhost:22000/instances/123/dimensions/age", body)
				r.Header.Set("If-Match", "wrong")
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(context.Context, string, string) (*models.Instance, error) {
					return nil, errs.ErrInstanceConflict
				}

				authorisationMock := &authMock.MiddlewareMock{
					RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
						return handlerFunc
					},
				}
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusConflict)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceConflict.Error())
				So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, "wrong")

				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}
