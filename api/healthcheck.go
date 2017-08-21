package api

import "net/http"

// HealthCheck returns the health of the application.
func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
