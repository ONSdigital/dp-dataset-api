package api

import (
	"net/http"

	"github.com/ONSdigital/go-ns/log"
)

// HealthCheck returns the health of the application.
func (api *DatasetAPI) healthCheck(w http.ResponseWriter, r *http.Request) {
	log.Debug("Healthcheck endpoint.", nil)
	w.WriteHeader(http.StatusOK)
}
