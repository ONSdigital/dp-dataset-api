package instance_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/smartystreets/goconvey/convey"
)

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

	convey.Convey("Given a dataset API with a successful store mock and auth", t, func() {
		instance := &models.Instance{
			InstanceID: "123",
		}
		mockedDataStore, isLocked := storeMockWithLock(instance, false)
		mockedDataStore.GetInstanceFunc = func(ctx context.Context, ID string, eTagSelector string) (*models.Instance, error) {
			return &models.Instance{
				InstanceID: ID,
				State:      models.CompletedState,
			}, nil
		}
		mockedDataStore.AddEventToInstanceFunc = func(ctx context.Context, currentInstance *models.Instance, event *models.Event, eTagSelector string) (string, error) {
			return testETag, nil
		}

		datasetPermissions := mocks.NewAuthHandlerMock()
		permissions := mocks.NewAuthHandlerMock()
		datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)

		convey.Convey("When a POST request to create an event for an instance resource is made, with a valid If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
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
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.AddEventToInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.AddEventToInstanceCalls()[0].CurrentInstance.InstanceID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.AddEventToInstanceCalls()[0].Event, convey.ShouldResemble, expectedEvent)
				convey.So(mockedDataStore.AddEventToInstanceCalls()[0].ETagSelector, convey.ShouldEqual, testIfMatch)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})

		convey.Convey("When a POST request to create an event for an instance resource is made, without an If-Match header", func() {
			body := strings.NewReader(bodyStr)
			r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
			convey.So(err, convey.ShouldBeNil)
			w := httptest.NewRecorder()
			datasetAPI.Router.ServeHTTP(w, r)

			convey.Convey("Then the response status is 200 OK, with the expected ETag header", func() {
				convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
				convey.So(w.Header().Get("ETag"), convey.ShouldEqual, testETag)
			})

			convey.Convey("Then the expected functions are called", func() {
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.AddEventToInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.AddEventToInstanceCalls()[0].CurrentInstance.InstanceID, convey.ShouldEqual, "123")
				convey.So(mockedDataStore.AddEventToInstanceCalls()[0].Event, convey.ShouldResemble, expectedEvent)
				convey.So(mockedDataStore.AddEventToInstanceCalls()[0].ETagSelector, convey.ShouldEqual, AnyETag)
			})

			convey.Convey("Then the db lock is acquired and released as expected", func() {
				validateLock(mockedDataStore, "123")
				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func TestAddEventToInstanceReturnsBadRequest(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a request to add an event to an instance resource contains an invalid body", t, func() {
		convey.Convey("When the request is made", func() {
			convey.Convey("Then the request fails and the response returns status bad requets (400)", func() {
				body := strings.NewReader(`{`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
			})
		})
	})

	convey.Convey("Given a request to add an event to an instance resource is missing the field `time` in request body", t, func() {
		convey.Convey("When the request is made", func() {
			convey.Convey("Then the request fails and the response returns status bad requets (400)", func() {
				body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00" }`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()
				mockedDataStore := &storetest.StorerMock{}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrMissingParameters.Error())
				convey.So(len(mockedDataStore.AddEventToInstanceCalls()), convey.ShouldEqual, 0)
			})
		})
	})
}

func TestAddEventToInstanceReturnsNotFound(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a valid request is made to add an instance event", t, func() {
		convey.Convey("When the instance does not exist", func() {
			convey.Convey("Then the request fails and the response returns status not found (404)", func() {
				body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ctx context.Context, ID string, eTagSelector string) (*models.Instance, error) {
					return nil, errs.ErrInstanceNotFound
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusNotFound)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInstanceNotFound.Error())
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func TestAddEventToInstanceReturnsInternalError(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a valid request to add instance event", t, func() {
		convey.Convey("When service is unable to connect to datastore", func() {
			convey.Convey("Then response return status internal server error (500)", func() {
				body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ctx context.Context, ID string, eTagSelector string) (*models.Instance, error) {
					return &models.Instance{
						InstanceID: ID,
						State:      models.CompletedState,
					}, nil
				}
				mockedDataStore.AddEventToInstanceFunc = func(ctx context.Context, currentInstance *models.Instance, event *models.Event, eTagSelector string) (string, error) {
					return "", errs.ErrInternalServer
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusInternalServerError)
				convey.So(w.Body.String(), convey.ShouldContainSubstring, errs.ErrInternalServer.Error())
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.AddEventToInstanceCalls(), convey.ShouldHaveLength, 1)

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}

func TestAddInstanceConflict(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a valid request to add an event to instance resource", t, func() {
		convey.Convey("When the request is made with an If-Match header that does not match the instance eTag", func() {
			convey.Convey("Then response return status conflict (409)", func() {
				body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
				r.Header.Set("If-Match", "wrong")
				convey.So(err, convey.ShouldBeNil)
				w := httptest.NewRecorder()

				instance := &models.Instance{
					InstanceID: "123",
				}
				mockedDataStore, isLocked := storeMockWithLock(instance, false)
				mockedDataStore.GetInstanceFunc = func(ctx context.Context, ID string, eTagSelector string) (*models.Instance, error) {
					return nil, errs.ErrInstanceConflict
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithCantabularMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				convey.So(w.Code, convey.ShouldEqual, http.StatusConflict)
				convey.So(mockedDataStore.GetInstanceCalls(), convey.ShouldHaveLength, 1)
				convey.So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, convey.ShouldEqual, "wrong")
				convey.So(mockedDataStore.GetInstanceCalls()[0].ID, convey.ShouldEqual, "123")

				convey.So(*isLocked, convey.ShouldBeFalse)
			})
		})
	})
}
