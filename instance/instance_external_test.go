package instance_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/instance"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/gorilla/mux"
	. "github.com/smartystreets/goconvey/convey"
)

const secretKey = "coffee"
const host = "http://locahost:8080"

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

		instance := &instance.Store{"http://lochost://8080", mockedDataStore}
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

		instance := &instance.Store{host, mockedDataStore}
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

		instance := &instance.Store{host, mockedDataStore}
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

		instance := &instance.Store{host, mockedDataStore}
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

		instance := &instance.Store{host, mockedDataStore}
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

		instance := &instance.Store{host, mockedDataStore}
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

		instance := &instance.Store{host, mockedDataStore}
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

		instance := &instance.Store{host, mockedDataStore}
		instance.Add(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.AddInstanceCalls()), ShouldEqual, 1)
	})
}

func TestUpdateInstanceReturnsOk(t *testing.T) {
	t.Parallel()
	Convey("when an instance has a state of created", t, func() {
		body := strings.NewReader(`{"state":"created"}`)
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
		w := httptest.NewRecorder()

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(id string) (*models.Instance, error) {
				return &models.Instance{}, nil
			},
			UpdateInstanceFunc: func(id string, i *models.Instance) error {
				return nil
			},
		}

		instance := &instance.Store{host, mockedDataStore}
		instance.Update(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
	})

	Convey("when an instance changes its state to edition-confirmed", t, func() {
		body := strings.NewReader(`{"state":"edition-confirmed", "edition": "2017"}`)
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
		w := httptest.NewRecorder()

		currentInstanceTestData := &models.Instance{
			Edition: "2017",
			Links: models.InstanceLinks{
				Dataset: models.IDLink{
					ID: "4567",
				},
			},
			State: "completed",
		}

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(id string) (*models.Instance, error) {
				return currentInstanceTestData, nil
			},
			UpdateInstanceFunc: func(id string, i *models.Instance) error {
				return nil
			},
			UpsertEditionFunc: func(datasetID, edition string, editionDoc *models.Edition) error {
				return nil
			},
			GetVersionByInstanceIDFunc: func(instanceID string) (*models.Version, error) {
				return nil, errs.VersionNotFound
			},
			GetNextVersionFunc: func(string, string) (int, error) {
				return 1, nil
			},
			UpsertVersionFunc: func(versionID string, version *models.Version) error {
				return nil
			},
		}

		instance := &instance.Store{host, mockedDataStore}
		instance.Update(w, r)

		So(w.Code, ShouldEqual, http.StatusOK)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionByInstanceIDCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetNextVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertVersionCalls()), ShouldEqual, 1)
	})
}

func TestUpdateInstanceFailure(t *testing.T) {
	t.Parallel()
	Convey("when the json body is in the incorrect structure return a bad request error", t, func() {
		body := strings.NewReader(`{"state":`)
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{}

		instance := &instance.Store{host, mockedDataStore}
		instance.Update(w, r)

		So(w.Code, ShouldEqual, http.StatusBadRequest)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 0)
	})

	Convey("when the instance does not exist return status not found", t, func() {
		body := strings.NewReader(`{"edition": "2017"}`)
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(id string) (*models.Instance, error) {
				return nil, errs.InstanceNotFound
			},
		}

		instance := &instance.Store{host, mockedDataStore}
		instance.Update(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 0)
	})
}

func TestUpdateInstanceReturnsInternalError(t *testing.T) {
	t.Parallel()
	Convey("update to an instance returns an internal error", t, func() {
		body := strings.NewReader(`{"state":"completed"}`)
		r := createRequestWithToken("PUT", "http://localhost:21800/instances/123", body)
		w := httptest.NewRecorder()

		currentInstanceTestData := &models.Instance{
			Edition: "2017",
			Links: models.InstanceLinks{
				Dataset: models.IDLink{
					ID: "4567",
				},
			},
			State: "completed",
		}

		mockedDataStore := &storetest.StorerMock{
			GetInstanceFunc: func(id string) (*models.Instance, error) {
				return currentInstanceTestData, nil
			},
			UpdateInstanceFunc: func(id string, i *models.Instance) error {
				return internalError
			},
		}

		instance := &instance.Store{host, mockedDataStore}
		instance.Update(w, r)

		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		So(len(mockedDataStore.GetInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 0)
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
		instance := &instance.Store{host, mockedDataStore}

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

		instance := &instance.Store{host, mockedDataStore}
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
				return errs.InstanceNotFound
			},
		}

		instance := &instance.Store{host, mockedDataStore}

		router := mux.NewRouter()
		router.HandleFunc("/instances/{id}/inserted_observations/{inserted_observations}", instance.UpdateObservations).Methods("PUT")
		router.ServeHTTP(w, r)

		So(w.Code, ShouldEqual, http.StatusNotFound)
		So(len(mockedDataStore.UpdateObservationInsertedCalls()), ShouldEqual, 1)
	})
}