package instance_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/api-errors"
	"github.com/ONSdigital/dp-dataset-api/instance"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

const secretKey = "coffee"

var internalError = errors.New("internal error")

func createRequestWithToken(method, url string, body io.Reader) *http.Request {
	r := httptest.NewRequest(method, url, body)
	r.Header.Add("internal-token", secretKey)
	return r
}

func TestGetInstancesReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("Get instances returns a ok status code", t, func() {
		r := createRequestWithToken("GET", "http://localhost:21800/instances", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstancesFunc: func(string) (*models.InstanceResults, error) {
				return &models.InstanceResults{}, nil
			},
		}

		instance := &instance.Store{mockedDataStore}
		instance.GetList(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstancesCalls()), ShouldEqual, 1)
	})
}

func TestGetInstancesReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Get instances returns an internal error", t, func() {
		r := createRequestWithToken("GET", "http://localhost:21800/instances", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstancesFunc: func(string) (*models.InstanceResults, error) {
				return nil, internalError
			},
		}

		instance := &instance.Store{mockedDataStore}
		instance.GetList(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstancesCalls()), ShouldEqual, 1)
	})
}

func TestGetInstanceReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("Get instance returns a ok status code", t, func() {
		r := createRequestWithToken("GET", "http://localhost:21800/instances/123", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return &models.Instance{}, nil
			},
		}

		instance := &instance.Store{mockedDataStore}
		instance.Get(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
	})
}

func TestGetInstanceReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Get instance returns an internal error", t, func() {
		r := createRequestWithToken("GET", "http://localhost:21800/instances/123", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(ID string) (*models.Instance, error) {
				return nil, internalError
			},
		}

		instance := &instance.Store{mockedDataStore}
		instance.Get(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddInstancesReturnsCreated(t *testing.T) {
	t.Parallel()
	Convey("Add instance returns a created code", t, func() {
		body := strings.NewReader(`{"links": { "job": { "id":"123-456", "href":"http://localhost:2200/jobs/123-456" } } }`)
		r := createRequestWithToken("POST", "http://localhost:21800/instances", body)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
				return &models.Instance{}, nil
			},
		}

		instance := &instance.Store{mockedDataStore}
		instance.Add(w, r)

		So(w.Code, ShouldEqual, http.StatusCreated)
		So(len(mockedDataStore.AddInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddInstancesReturnsBadRequest(t *testing.T) {
	t.Parallel()
	Convey("Add instance returns a bad request with invalid json", t, func() {
		body := strings.NewReader(`{`)
		r := createRequestWithToken("POST", "http://localhost:21800/instances", body)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
				return &models.Instance{}, nil
			},
		}

		instance := &instance.Store{mockedDataStore}
		instance.Add(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})
	Convey("Add instance returns a bad request with a empty json", t, func() {
		body := strings.NewReader(`{}`)
		r := createRequestWithToken("POST", "http://localhost:21800/instances", body)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
				return &models.Instance{}, nil
			},
		}

		instance := &instance.Store{mockedDataStore}
		instance.Add(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})
}

func TestAddInstancesReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Add instance returns an internal error", t, func() {
		body := strings.NewReader(`{"links": {"job": { "id":"123-456", "href":"http://localhost:2200/jobs/123-456" } } }`)
		r := createRequestWithToken("POST", "http://localhost:21800/instances", body)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			AddInstanceFunc: func(*models.Instance) (*models.Instance, error) {
				return nil, internalError
			},
		}

		instance := &instance.Store{mockedDataStore}
		instance.Add(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.AddInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddDimensionToInstanceReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Add a dimension to an instance returns ok", t, func() {
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			AddDimensionToInstanceFunc: func(event *models.Dimension) error {
				return nil
			},
		}

		instance := &instance.Store{mockedDataStore}
		instance.AddDimension(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddDimensionToInstanceReturnsNotFound(t *testing.T) {
	t.Parallel()
	Convey("Add a dimension to an instance returns not found", t, func() {
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			AddDimensionToInstanceFunc: func(event *models.Dimension) error {
				return api_errors.DimensionNodeNotFound
			},
		}

		instance := &instance.Store{mockedDataStore}
		instance.AddDimension(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAddDimensionToInstanceReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("Add a dimension to an instance returns internal error", t, func() {
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123/dimensions/age/options/55", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			AddDimensionToInstanceFunc: func(event *models.Dimension) error {
				return internalError
			},
		}

		instance := &instance.Store{mockedDataStore}
		instance.AddDimension(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.AddDimensionToInstanceCalls()), ShouldEqual, 1)
	})
}

func TestUpdateInstanceReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("update to an instance returns an internal error", t, func() {
		body := strings.NewReader(`{"state":"completed"}`)
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			UpdateInstanceFunc: func(id string, i *models.Instance) error {
				return nil
			},
		}

		instance := &instance.Store{mockedDataStore}
		instance.Update(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
	})
}

func TestUpdateInstanceReturnsBadRequest(t *testing.T) {
	t.Parallel()
	Convey("update to an instance returns an bad request error", t, func() {
		body := strings.NewReader(`{"state":`)
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		instance := &instance.Store{mockedDataStore}
		instance.Update(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})
}

func TestUpdateInstanceReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("update to an instance returns an internal error", t, func() {
		body := strings.NewReader(`{"state":"completed"}`)
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			UpdateInstanceFunc: func(id string, i *models.Instance) error {
				return internalError
			},
		}

		instance := &instance.Store{mockedDataStore}
		instance.Update(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
	})
}

func TestInsertedObservationsReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("Updateding the inserted observations returns ok", t, func() {
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			UpdateObservationInsertedFunc: func(id string, ob int64) error {
				return nil
			},
		}
		instance := &instance.Store{mockedDataStore}

		router := mux.NewRouter()
		router.HandleFunc("/instances/{id}/inserted_observations/{inserted_observations}", instance.UpdateObservations).Methods("PUT")
		router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 1)
	})
}

func TestInsertedObservationsReturnsBadRequest(t *testing.T) {
	t.Parallel()
	Convey("Updateding the inserted observations returns bad request", t, func() {
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/aa12a", nil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		instance := &instance.Store{mockedDataStore}
		instance.UpdateObservations(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
	})
}

func TestInsertedObservationsReturnsNotFound(t *testing.T) {
	t.Parallel()
	Convey("Updating the inserted observations returns not found", t, func() {
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123/inserted_observations/200", nil)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			UpdateObservationInsertedFunc: func(id string, ob int64) error {
				return api_errors.InstanceNotFound
			},
		}

		instance := &instance.Store{mockedDataStore}

		router := mux.NewRouter()
		router.HandleFunc("/instances/{id}/inserted_observations/{inserted_observations}", instance.UpdateObservations).Methods("PUT")
		router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 1)
	})
}
