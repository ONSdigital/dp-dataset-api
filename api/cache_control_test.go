package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPublicCacheControlMiddleware(t *testing.T) {
	Convey("Given the public cache-control middleware", t, func() {
		request := httptest.NewRequest(http.MethodGet, "/datasets/dataset-id", http.NoBody)
		recorder := httptest.NewRecorder()

		Convey("When the handler returns 200 OK", func() {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				So(r, ShouldEqual, request)
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte(`{"message":"response"}`))
				So(err, ShouldBeNil)
			})

			publicCacheControlMiddleware(handler).ServeHTTP(recorder, request)

			response := recorder.Result()
			defer response.Body.Close()

			So(response.Header.Get("Cache-Control"), ShouldEqual, "stale-while-revalidate=0, stale-if-error=30")
			So(response.StatusCode, ShouldEqual, http.StatusOK)
			So(recorder.Body.String(), ShouldEqual, `{"message":"response"}`)
		})

		Convey("When the handler returns 404 Not Found", func() {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				So(r, ShouldEqual, request)
				w.WriteHeader(http.StatusNotFound)
				_, err := w.Write([]byte("dataset not found"))
				So(err, ShouldBeNil)
			})

			publicCacheControlMiddleware(handler).ServeHTTP(recorder, request)

			response := recorder.Result()
			defer response.Body.Close()

			So(response.Header.Get("Cache-Control"), ShouldEqual, "stale-while-revalidate=0, stale-if-error=30")
			So(response.StatusCode, ShouldEqual, http.StatusNotFound)
			So(recorder.Body.String(), ShouldEqual, "dataset not found")
		})
	})
}
