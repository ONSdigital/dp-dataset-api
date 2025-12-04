package instance_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	authMock "github.com/ONSdigital/dp-authorisation/v2/authorisation/mock"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAddEventUnauthorised(t *testing.T) {
	t.Parallel()

	bodyStr := `{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829Z" }`

	Convey("Given a dataset API with a successful store mock and auth that returns unauthorised", t, func() {

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusUnauthorized)
				}

			},
		}

		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)

		Convey("When a POST request to create an event for an instance resource is made, with a valid If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithNoToken("POST", "http://localhost:21800/instances/123/events", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 401 unauthorised", func() {
				So(w.Code, ShouldEqual, http.StatusUnauthorized)
			})

			Convey("Then none of the expected functions are called", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 0)
				So(mockedDataStore.AddEventToInstanceCalls(), ShouldHaveLength, 0)
			})
		})
	})
}

func TestAddEventForbidden(t *testing.T) {
	t.Parallel()

	bodyStr := `{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829Z" }`

	Convey("Given a dataset API with a successful store mock and auth that returns forbidden", t, func() {

		mockedDataStore := &storetest.StorerMock{}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
				}

			},
		}

		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)

		Convey("When a POST request to create an event for an instance resource is made, with a valid If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
			r.Header.Set("If-Match", testIfMatch)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 403 forbidden", func() {
				So(w.Code, ShouldEqual, http.StatusForbidden)
			})

			Convey("Then none of the expected functions are called", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 0)
				So(mockedDataStore.AddEventToInstanceCalls(), ShouldHaveLength, 0)
			})
		})
	})
}

func TestAddEventReturnsOk(t *testing.T) {
	t.Parallel()

	bodyStr := `{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829Z" }`
	layout := "2006-01-02T15:04:05.000Z"
	str := "2017-08-25T15:09:11.829Z"
	testTime, _ := time.Parse(layout, str)
	expectedEvent := &models.Event{
		Message:       "321",
		Type:          "error",
		MessageOffset: "00",
		Time:          &testTime,
	}

	Convey("Given a dataset API with a successful store mock and auth", t, func() {
		instance := &models.Instance{
			InstanceID: "123",
		}
		mockedDataStore, isLocked := storeMockWithLock(instance, false)
		mockedDataStore.GetInstanceFunc = func(_ context.Context, ID string, _ string) (*models.Instance, error) {
			return &models.Instance{
				InstanceID: ID,
				State:      models.CompletedState,
			}, nil
		}
		mockedDataStore.AddEventToInstanceFunc = func(context.Context, *models.Instance, *models.Event, string) (string, error) {
			return testETag, nil
		}

		authorisationMock := &authMock.MiddlewareMock{
			RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
				return handlerFunc
			},
		}

		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)

		Convey("When a POST request to create an event for an instance resource is made, with a valid If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
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
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.AddEventToInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.AddEventToInstanceCalls()[0].CurrentInstance.InstanceID, ShouldEqual, "123")
				So(mockedDataStore.AddEventToInstanceCalls()[0].Event, ShouldResemble, expectedEvent)
				So(mockedDataStore.AddEventToInstanceCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})

		Convey("When a POST request to create an event for an instance resource is made, without an If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
			})

			Convey("Then the expected functions are called", func() {
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, AnyETag)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.AddEventToInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.AddEventToInstanceCalls()[0].CurrentInstance.InstanceID, ShouldEqual, "123")
				So(mockedDataStore.AddEventToInstanceCalls()[0].Event, ShouldResemble, expectedEvent)
				So(mockedDataStore.AddEventToInstanceCalls()[0].ETagSelector, ShouldEqual, AnyETag)
			})

			Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func TestAddEventToInstanceReturnsBadRequest(t *testing.T) {
	t.Parallel()
	Convey("Given a request to add an event to an instance resource contains an invalid body", t, func() {
		Convey("When the request is made", func() {
			Convey("Then the request fails and the response returns status bad requets (400)", func() {
				body := strings.NewReader(`{`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{}

				authorisationMock := &authMock.MiddlewareMock{
					RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
						return handlerFunc
					},
				}

				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
			})
		})
	})

	Convey("Given a request to add an event to an instance resource is missing the field `time` in request body", t, func() {
		Convey("When the request is made", func() {
			Convey("Then the request fails and the response returns status bad requets (400)", func() {
				body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00" }`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{}

				authorisationMock := &authMock.MiddlewareMock{
					RequireFunc: func(permission string, handlerFunc http.HandlerFunc) http.HandlerFunc {
						return handlerFunc
					},
				}
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, authorisationMock)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrMissingParameters.Error())
				So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 0)
			})
		})
	})
}

func TestAddEventToInstanceReturnsNotFound(t *testing.T) {
	t.Parallel()
	Convey("Given a valid request is made to add an instance event", t, func() {
		Convey("When the instance does not exist", func() {
			Convey("Then the request fails and the response returns status not found (404)", func() {
				body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
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
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)

				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func TestAddEventToInstanceReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Given a valid request to add instance event", t, func() {
		Convey("When service is unable to connect to datastore", func() {
			Convey("Then response return status internal server error (500)", func() {
				body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(_ context.Context, ID string, _ string) (*models.Instance, error) {
					return &models.Instance{
						InstanceID: ID,
						State:      models.CompletedState,
					}, nil
				}
				mockedDataStore.AddEventToInstanceFunc = func(context.Context, *models.Instance, *models.Event, string) (string, error) {
					return "", errs.ErrInternalServer
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
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.AddEventToInstanceCalls(), ShouldHaveLength, 1)

				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}

func TestAddInstanceConflict(t *testing.T) {
	t.Parallel()
	Convey("Given a valid request to add an event to instance resource", t, func() {
		Convey("When the request is made with an If-Match header that does not match the instance eTag", func() {
			Convey("Then response return status conflict (409)", func() {
				body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
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
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, "wrong")
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")

				So(*isLocked, ShouldBeFalse)
			})
		})
	})
}
