package api

import (
	"testing"
	"net/http"
	"net/http/httptest"
	"github.com/ONSdigital/dp-dataset-api/api/datastoretest"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/ONSdigital/dp-dataset-api/models"
	"strings"
)


func TestGetInstancesReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("Get instances returns a ok status code", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/instances", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetInstancesFunc: func() (*models.InstanceResults, error) {
				return &models.InstanceResults{}, nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstancesCalls()), ShouldEqual, 1)
	})
}

func TestGetInstancesReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Get instances returns an internal error", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/instances", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetInstancesFunc: func() (*models.InstanceResults, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstancesCalls()), ShouldEqual, 1)
	})
}

func TestGetInstanceReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("Get instance returns a ok status code", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/instances/123", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{}, nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
	})
}

func TestGetInstanceReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Get instance returns an internal error", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/instances/123", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddInstancesReturnsCreated(t *testing.T) {
	t.Parallel()
	Convey("Add instance returns a created code", t, func() {
		body := strings.NewReader(`{"job": { "id":"123-456", "link":"http://localhost:2200/jobs/123-456" } }`)
		r, err := http.NewRequest("POST", "http://localhost:21800/instances", body)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
				return &models.Instance{}, nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusCreated)
		So(len(mockedDataStore.AddInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddInstancesReturnsBadRequest(t *testing.T) {
	t.Parallel()
	Convey("Add instance returns a bad request with invalid json", t, func() {
		body := strings.NewReader(`{`)
		r, err := http.NewRequest("POST", "http://localhost:21800/instances", body)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
				return &models.Instance{}, nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})
	Convey("Add instance returns a bad request with a empty json", t, func() {
		body := strings.NewReader(`{}`)
		r, err := http.NewRequest("POST", "http://localhost:21800/instances", body)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
				return &models.Instance{}, nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})
}

func TestAddInstancesReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Add instance returns an internal error", t, func() {
		body := strings.NewReader(`{"job": { "id":"123-456", "link":"http://localhost:2200/jobs/123-456" } }`)
		r, err := http.NewRequest("POST", "http://localhost:21800/instances", body)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.AddInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddEventToInstanceReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`)
		r, err := http.NewRequest("POST", "http://localhost:21800/instances/123/events", body)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			AddEventToInstanceFunc: func(id string, event *models.Event) (error) {
				return nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddEventToInstanceReturnsBadRequest(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00" }`)
		r, err := http.NewRequest("POST", "http://localhost:21800/instances/123/events", body)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})
	Convey("", t, func() {
		body := strings.NewReader(`{`)
		r, err := http.NewRequest("POST", "http://localhost:21800/instances/123/events", body)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})
}

func TestAddEventToInstanceReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		body := strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`)
		r, err := http.NewRequest("POST", "http://localhost:21800/instances/123/events", body)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			AddEventToInstanceFunc: func(id string, event *models.Event) (error) {
				return internalError
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 1)
	})
}
