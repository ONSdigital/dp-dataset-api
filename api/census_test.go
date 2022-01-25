package api

import (
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/gorilla/mux"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAPIRouteRegistration(t *testing.T) {

	Convey("Given the data set API is created", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)
		api := Setup(testContext, cfg, mux.NewRouter(), store.DataStore{}, urlBuilder, nil, nil, nil)

		Convey("When I GET /census", func() {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/census", nil)
			api.Router.ServeHTTP(rec, req)

			SoMsg("Then it should return with 200",
				rec.Code, ShouldEqual, 200)
			SoMsg("Then it should return application/json content",
				rec.Header().Get("Content-Type"), ShouldEqual, "application/json")
		})
	})
}
