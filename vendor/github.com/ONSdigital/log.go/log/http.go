package log

import (
	"net/http"
	"strconv"
	"time"
)

// EventHTTP is the data structure used for logging a HTTP event.
//
// It isn't very useful to export, other than for documenting the
// data structure it outputs.
type EventHTTP struct {
	StatusCode *int   `json:"status_code,omitempty"`
	Method     string `json:"method,omitempty"`

	// URL data
	Scheme string `json:"scheme,omitempty"`
	Host   string `json:"host,omitempty"`
	Port   int    `json:"port,omitempty"`
	Path   string `json:"path,omitempty"`
	Query  string `json:"query,omitempty"`

	// Timing data
	StartedAt             *time.Time     `json:"started_at,omitempty"`
	EndedAt               *time.Time     `json:"ended_at,omitempty"`
	Duration              *time.Duration `json:"duration,omitempty"`
	ResponseContentLength int64          `json:"response_content_length,omitempty"`
}

func (l *EventHTTP) attach(le *EventData) {
	le.HTTP = l
}

// HTTP returns an option you can pass to Event to log HTTP
// request data with a log event.
//
// It converts the port number to a integer if possible, otherwise
// the port number is 0.
//
// It splits the URL into its component parts, and stores the scheme,
// host, port, path and query string individually.
//
// It also calculates the duration if both startedAt and endedAt are
// passed in, for example when wrapping a http.Handler.
func HTTP(req *http.Request, statusCode int, responseContentLength int64, startedAt, endedAt *time.Time) option {
	port := 0
	if p := req.URL.Port(); len(p) > 0 {
		port, _ = strconv.Atoi(p)
	}

	var duration *time.Duration
	if startedAt != nil && endedAt != nil {
		d := endedAt.Sub(*startedAt)
		duration = &d
	}

	return &EventHTTP{
		StatusCode: &statusCode,
		Method:     req.Method,

		Scheme: req.URL.Scheme,
		Host:   req.URL.Hostname(),
		Port:   port,
		Path:   req.URL.Path,
		Query:  req.URL.RawQuery,

		StartedAt:             startedAt,
		EndedAt:               endedAt,
		Duration:              duration,
		ResponseContentLength: responseContentLength,
	}
}
