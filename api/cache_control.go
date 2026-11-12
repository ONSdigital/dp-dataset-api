package api

import (
	"fmt"
	"net/http"
	"strings"
)

// publicCacheControlMiddleware sets the Cache-Control header for all public responses.
// Negative directive values are omitted.
func publicCacheControlMiddleware(staleWhileRevalidateSeconds, staleIfErrorSeconds int) func(http.Handler) http.Handler {
	var directives []string
	if staleWhileRevalidateSeconds >= 0 {
		directives = append(directives, fmt.Sprintf("stale-while-revalidate=%d", staleWhileRevalidateSeconds))
	}
	if staleIfErrorSeconds >= 0 {
		directives = append(directives, fmt.Sprintf("stale-if-error=%d", staleIfErrorSeconds))
	}
	cacheControlHeaderValue := strings.Join(directives, ", ")

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cacheControlHeaderValue != "" {
				w.Header().Set("Cache-Control", cacheControlHeaderValue)
			}
			next.ServeHTTP(w, r)
		})
	}
}
