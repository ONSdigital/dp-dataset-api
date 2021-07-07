package instance_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAddEventReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Given a valid request to add an event to instance resource", t, func() {
		Convey("When the request is made", func() {
			Convey(`Then the instance is successfully updated with the event and a
				response status ok (200) is returned`, func() {
				body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`)
				r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
				r.Header.Set("If-Match", testIfMatch)
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
						return &models.Instance{
							InstanceID: ID,
							State:      models.CompletedState,
						}, nil
					},
					AddEventToInstanceFunc: func(currentInstance *models.Instance, event *models.Event, eTagSelector string) (string, error) {
						return testETag, nil
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(w.Header().Get("ETag"), ShouldEqual, testETag)
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
				So(mockedDataStore.AddEventToInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.AddEventToInstanceCalls()[0].ETagSelector, ShouldEqual, testIfMatch)
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

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
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

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
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

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
						return nil, errs.ErrInstanceNotFound
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
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

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
						return &models.Instance{
							InstanceID: ID,
							State:      models.CompletedState,
						}, nil
					},
					AddEventToInstanceFunc: func(currentInstance *models.Instance, event *models.Event, eTagSelector string) (string, error) {
						return "", errs.ErrInternalServer
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.AddEventToInstanceCalls(), ShouldHaveLength, 1)
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

				mockedDataStore := &storetest.StorerMock{
					GetInstanceFunc: func(ID string, eTagSelector string) (*models.Instance, error) {
						return nil, errs.ErrInstanceConflict
					},
				}

				datasetPermissions := mocks.NewAuthHandlerMock()
				permissions := mocks.NewAuthHandlerMock()
				datasetAPI := getAPIWithMocks(testContext, mockedDataStore, &mocks.DownloadsGeneratorMock{}, datasetPermissions, permissions)
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusConflict)
				So(mockedDataStore.GetInstanceCalls(), ShouldHaveLength, 1)
				So(mockedDataStore.GetInstanceCalls()[0].ETagSelector, ShouldEqual, "wrong")
				So(mockedDataStore.GetInstanceCalls()[0].ID, ShouldEqual, "123")
			})
		})
	})
}
