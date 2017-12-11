package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ONSdigital/dp-dataset-api/store"
	"github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/gorilla/mux"

	"github.com/ONSdigital/dp-dataset-api/url"
	. "github.com/smartystreets/goconvey/convey"
)

func TestHealthCheckReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("test healthy healthcheck", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/healthcheck", nil)
		So(err, ShouldBeNil)
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			PingFunc: func(ctx context.Context) (time.Time, error) {
				return time.Now(), nil
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, url.NewBuilder("localhost:20000"))
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusOK)
		body := w.Body.String()
		So(body, ShouldContainSubstring, `"status":"OK"`)
	})
}

func TestHealthCheckReturnsError(t *testing.T) {
	t.Parallel()
	Convey("test unhealthy healthcheck", t, func() {
		r, err := http.NewRequest("GET", "http://localhost:22000/healthcheck", nil)
		So(err, ShouldBeNil)
		ourError := "Kaboom"
		w := httptest.NewRecorder()
		mockedDataStore := &storetest.StorerMock{
			PingFunc: func(ctx context.Context) (time.Time, error) {
				return time.Now(), errors.New(ourError)
			},
		}

		api := routes(host, secretKey, mux.NewRouter(), store.DataStore{Backend: mockedDataStore}, url.NewBuilder("localhost:20000"))
		api.router.ServeHTTP(w, r)
		So(w.Code, ShouldEqual, http.StatusInternalServerError)
		body := w.Body.String()
		So(body, ShouldContainSubstring, `"status":"error"`)
		So(body, ShouldContainSubstring, `"error":"`+ourError+`"`)
	})
}
