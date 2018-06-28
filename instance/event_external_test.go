package instance_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/instance"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	. "github.com/smartystreets/goconvey/convey"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
)

func TestAddEventReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Add an event to an instance returns ok", t, func() {
		body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			AddEventToInstanceFunc: func(id string, event *models.Event) error {
				return nil
			},
		}

		instance := &instance.Store{Host: host, Storer: mockedDataStore}
		instance.AddEvent(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddEventToInstanceReturnsBadRequest(t *testing.T) {
	t.Parallel()
	Convey("Add an event to an instance returns bad request", t, func() {
		body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00" }`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		instance := &instance.Store{Host: host, Storer: mockedDataStore}
		instance.AddEvent(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})

	Convey("Add an event to an instance returns bad request", t, func() {
		body := strings.NewReader(`{`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		instance := &instance.Store{Host: host, Storer: mockedDataStore}
		instance.AddEvent(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})
}

func TestAddEventToInstanceReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Add an event to an instance returns internal error", t, func() {
		body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`)
		r, err := createRequestWithToken("POST", "http://localhost:21800/instances/123/events", body)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			AddEventToInstanceFunc: func(id string, event *models.Event) error {
				return errs.ErrInternalServer
			},
		}

		instance := &instance.Store{Host: host, Storer: mockedDataStore}
		instance.AddEvent(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 1)
	})
}
