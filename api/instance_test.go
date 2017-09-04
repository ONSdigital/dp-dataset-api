package api

import (
	"github.com/ONSdigital/dp-dataset-api/api-errors"
	"github.com/ONSdigital/dp-dataset-api/api/datastoretest"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
			AddEventToInstanceFunc: func(id string, event *models.Event) error {
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
			AddEventToInstanceFunc: func(id string, event *models.Event) error {
				return internalError
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.AddEventToInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddDimensionToInstanceReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			AddDimensionToInstanceFunc: func(id string, event *models.DimensionNode) error {
				return nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddDimensionToInstanceReturnsNotFound(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			AddDimensionToInstanceFunc: func(id string, event *models.DimensionNode) error {
				return api_errors.DimensionNodeNotFound
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddDimensionToInstanceReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			AddDimensionToInstanceFunc: func(id string, event *models.DimensionNode) error {
				return internalError
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddNodeIDToDimensionReturnsOK(t *testing.T)  {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			AddDimensionToInstanceFunc: func(id string, event *models.DimensionNode) error {
				return nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddNodeIDToDimensionReturnsBadRequest(t *testing.T)  {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			AddDimensionToInstanceFunc: func(id string, event *models.DimensionNode) error {
				return api_errors.DimensionNodeNotFound
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddNodeIDToDimensionReturnsInternalError(t *testing.T)  {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55/node_id/11", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			AddDimensionToInstanceFunc: func(id string, event *models.DimensionNode) error {
				return internalError
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)
	})
}

func TestInsertObservationsReturnsOk(t *testing.T)  {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpdateObservationInsertedFunc: func(id string, ob int64) error {
				return nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 1)
	})
}

func TestInsertObservationsReturnsBadRequest(t *testing.T)  {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/inserted_observations/aa12a", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})
}

func TestInsertObservationsReturnsNotFound(t *testing.T)  {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			UpdateObservationInsertedFunc: func(id string, ob int64) error {
				return api_errors.InstanceNotFound
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 1)
	})
}

func TestGetDimensionNodesReturnsOk(t *testing.T)  {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetDimensionNodesFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return &models.DimensionNodeResults{}, nil
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetDimensionNodesFromInstanceCalls()), ShouldEqual, 1)
	})
}

func TestGetDimensionNodesReturnsNotFound(t *testing.T)  {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetDimensionNodesFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return nil, api_errors.InstanceNotFound
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetDimensionNodesFromInstanceCalls()), ShouldEqual, 1)
	})
}

func TestGetDimensionNodesReturnsInternalError(t *testing.T)  {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:21800/instances/123/dimensions", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{
			GetDimensionNodesFromInstanceFunc: func(id string) (*models.DimensionNodeResults, error) {
				return nil, internalError
			},
		}

		api := CreateDatasetAPI("123", mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetDimensionNodesFromInstanceCalls()), ShouldEqual, 1)
	})
}