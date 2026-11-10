package api

import (
	"fmt"
	"net/http"
)

// publicCacheControlMiddleware sets the Cache-Control header for all public responses.
func publicCacheControlMiddleware(staleWhileRevalidateSeconds, staleIfErrorSeconds int) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", fmt.Sprintf("stale-while-revalidate=%d, stale-if-error=%d", staleWhileRevalidateSeconds, staleIfErrorSeconds))
			next.ServeHTTP(w, r)
		})
	}
}
