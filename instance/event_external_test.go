package instance_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/instance"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/go-ns/audit"
	"github.com/ONSdigital/go-ns/audit/auditortest"
	"github.com/ONSdigital/go-ns/common"
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
				So(err, ShouldBeNil)
				w := httptest.NewRecorder()

				mockedDataStore := &storetest.StorerMock{
					AddEventToInstanceFunc: func(id string, event *models.Event) error {
						return nil
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusOK)
				So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Successful, common.Params{"instance_id": "123"}),
				)
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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
				So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Unsuccessful, common.Params{"instance_id": "123"}),
				)
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

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusBadRequest)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrMissingParameters.Error())
				So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Unsuccessful, common.Params{"instance_id": "123"}),
				)
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
					AddEventToInstanceFunc: func(id string, event *models.Event) error {
						return errs.ErrInstanceNotFound
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusNotFound)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInstanceNotFound.Error())
				So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Unsuccessful, common.Params{"instance_id": "123"}),
				)
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
					AddEventToInstanceFunc: func(id string, event *models.Event) error {
						return errs.ErrInternalServer
					},
				}

				auditor := auditortest.New()
				datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
				datasetAPI.Router.ServeHTTP(w, r)

				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Unsuccessful, common.Params{"instance_id": "123"}),
				)
			})
		})
	})
}

func TestAddInstanceEventAuditErrors(t *testing.T) {
	t.Parallel()
	Convey("Given audit action attempted returns an error", t, func() {

		auditor := auditortest.NewErroring(instance.AddInstanceEventAction, audit.Attempted)

		Convey("When add instance event is called", func() {
			body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`)
			r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 0)

				auditParams := common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}
				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Attempted, auditParams),
				)
			})
		})
	})

	Convey("Given audit action unsuccessful returns an error", t, func() {
		auditor := auditortest.NewErroring(instance.AddInstanceEventAction, audit.Unsuccessful)

		Convey("When add instance event returns an error", func() {
			r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", strings.NewReader(`{}`))
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("Then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 0)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Unsuccessful, common.Params{"instance_id": "123"}),
				)
			})
		})
	})

	Convey("given audit action successful returns an error", t, func() {
		auditor := auditortest.NewErroring(instance.AddInstanceEventAction, audit.Successful)

		Convey("when get instances is called", func() {
			body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`)
			r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
			So(err, ShouldBeNil)
			w := httptest.NewRecorder()

			mockedDataStore := &storetest.StorerMock{
				AddEventToInstanceFunc: func(id string, event *models.Event) error {
					return nil
				},
			}

			datasetAPI := getAPIWithMockedDatastore(mockedDataStore, &mocks.DownloadsGeneratorMock{}, auditor, &mocks.ObservationStoreMock{})
			datasetAPI.Router.ServeHTTP(w, r)

			Convey("then a 500 status is returned", func() {
				So(w.Code, ShouldEqual, http.StatusInternalServerError)
				So(w.Body.String(), ShouldContainSubstring, errs.ErrInternalServer.Error())
				So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 1)

				auditor.AssertRecordCalls(
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Attempted, common.Params{"caller_identity": "someone@ons.gov.uk", "instance_id": "123"}),
					auditortest.NewExpectation(instance.AddInstanceEventAction, audit.Successful, common.Params{"instance_id": "123"}),
				)
			})
		})
	})
}
