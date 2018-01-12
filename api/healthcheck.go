package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ONSdigital/go-ns/log"
)

var (
	lastSuccess time.Time
	lastChecked time.Time
)

// healthMessage models the json returned by a healthcheck endpoint
type healthMessage struct {
	Status      string    `json:"status"`
	Error       string    `json:"error,omitempty"`
	LastSuccess time.Time `json:"last_success,omitempty"`
	LastChecked time.Time `json:"last_checked,omitempty"`
}

type healthResult struct {
	Error       error
	LastChecked time.Time
}

// healthCheck asks each service its health, and returns any error
func (api *DatasetAPI) healthCheck(w http.ResponseWriter, r *http.Request) {
	var (
		body        []byte
		healthIssue string
		err         error
		wg          sync.WaitGroup
		status      = http.StatusOK
		healthiness = healthMessage{Status: "OK"}
	)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	healthChan := make(chan healthResult)

	// test db access
	wg.Add(1)
	go func() {
		var lastPing time.Time
		lastPing, err = api.dataStore.Backend.Ping(r.Context())
		healthChan <- healthResult{Error: err, LastChecked: lastPing}
		wg.Done()
		if err != nil {
			log.ErrorC("healthcheck", err, nil)
		}
	}()

	// setup close() when all checks complete (avoid sending on closed channel)
	go func() {
		wg.Wait()
		close(healthChan)
	}()

	select {
	case res := <-healthChan:
		if res.Error != nil {
			healthIssue = res.Error.Error()
		} else {
			lastSuccess = res.LastChecked
		}
		lastChecked = res.LastChecked
	case <-r.Context().Done():
		return
	case <-time.After(api.healthCheckTimeout):
		healthIssue = "timeout waiting for db response"
	}

	// when healthIssue detected, change headers and content from default "OK"
	if healthIssue != "" {
		status = http.StatusInternalServerError
		healthiness.Status = "error"
		healthiness.Error = healthIssue
	}
	// populate result with times
	healthiness.LastSuccess = lastSuccess
	healthiness.LastChecked = lastChecked

	// convert to json response
	if body, err = json.Marshal(healthiness); err != nil {
		log.ErrorC("marshal", err, log.Data{"health": healthiness})
		panic(err)
	}

	// return status and json body
	w.WriteHeader(status)
	fmt.Fprintf(w, "%s", body)
}
