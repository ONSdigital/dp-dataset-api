package api

import (
	"testing"
	"net/http"
	"net/http/httptest"
	"github.com/ONSdigital/dp-dataset-api/api/datastoretest"
	"github.com/gorilla/mux"

	. "github.com/smartystreets/goconvey/convey"
)

func TestHealthCheckReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/healthcheck", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &backendtest.BackendMock{}

		api := CreateDatasetAPI(secretKey, mux.NewRouter(), DataStore{Backend: mockedDataStore})
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
	})
}
