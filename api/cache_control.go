package api

import "net/http"

// Cache-Control header constants
const (
	staleWhileRevalidate = "stale-while-revalidate=0"
	staleIfError         = "stale-if-error=30"
)

// publicCacheControlMiddleware sets the Cache-Control header for all public responses.
func publicCacheControlMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", staleWhileRevalidate+", "+staleIfError)
		next.ServeHTTP(w, r)
	})
}
