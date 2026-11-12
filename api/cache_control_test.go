package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPublicCacheControlMiddleware(t *testing.T) {
	tests := []struct {
		name                        string
		staleWhileRevalidateSeconds int
		staleIfErrorSeconds         int
		expectedCacheControl        string
	}{
		{
			name:                        "both directives enabled",
			staleWhileRevalidateSeconds: 60,
			staleIfErrorSeconds:         120,
			expectedCacheControl:        "stale-while-revalidate=60, stale-if-error=120",
		},
		{
			name:                        "both directives zero",
			staleWhileRevalidateSeconds: 0,
			staleIfErrorSeconds:         0,
			expectedCacheControl:        "stale-while-revalidate=0, stale-if-error=0",
		},
		{
			name:                        "stale-while-revalidate disabled",
			staleWhileRevalidateSeconds: -1,
			staleIfErrorSeconds:         30,
			expectedCacheControl:        "stale-if-error=30",
		},
		{
			name:                        "stale-if-error disabled",
			staleWhileRevalidateSeconds: 60,
			staleIfErrorSeconds:         -1,
			expectedCacheControl:        "stale-while-revalidate=60",
		},
		{
			name:                        "both directives disabled",
			staleWhileRevalidateSeconds: -1,
			staleIfErrorSeconds:         -1,
			expectedCacheControl:        "",
		},
	}

	for _, tt := range tests {
		Convey("Given "+tt.name, t, func() {
			request := httptest.NewRequest(http.MethodGet, "/datasets", http.NoBody)
			recorder := httptest.NewRecorder()
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			Convey("When the cache-control middleware handles the request", func() {
				publicCacheControlMiddleware(tt.staleWhileRevalidateSeconds, tt.staleIfErrorSeconds)(handler).ServeHTTP(recorder, request)

				Convey("Then the Cache-Control header matches the expected value", func() {
					So(recorder.Header().Get("Cache-Control"), ShouldEqual, tt.expectedCacheControl)
				})
			})
		})
	}
}
