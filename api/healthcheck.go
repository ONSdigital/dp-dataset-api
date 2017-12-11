package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/ONSdigital/go-ns/log"
)

// healthMessage models the json returned by a healthcheck endpoint
type healthMessage struct {
	Status      string    `json:"status"`
	Error       string    `json:"error,omitempty"`
	LastChecked time.Time `json:"last_checked"`
}

// healthCheck asks each service its health, and returns any error
func (api *DatasetAPI) healthCheck(w http.ResponseWriter, r *http.Request) {
	var (
		body        []byte
		healthIssue string
		status      = http.StatusOK
		healthiness = healthMessage{Status: "OK"}
	)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	// test db access
	lastPing, err := api.dataStore.Backend.Ping(r.Context())
	if err != nil {
		healthIssue = err.Error()
	}

	// when healthIssue detected, change headers and content
	if healthIssue != "" {
		status = http.StatusInternalServerError
		healthiness.Status = "error"
		healthiness.Error = healthIssue
	}
	healthiness.LastChecked = lastPing

	if body, err = json.Marshal(healthiness); err != nil {
		log.ErrorC("marshal", err, log.Data{"health": healthiness})
		panic(err)
	}

	// return status and json body
	w.WriteHeader(status)
	fmt.Fprintf(w, "%s", body)
}
