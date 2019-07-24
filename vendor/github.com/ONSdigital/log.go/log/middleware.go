package log

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"time"
)

// Middleware implements the logger middleware and captures HTTP request data
//
// It implements http.Handler, and wraps an inbound HTTP request to log useful
// information including the URL, request start/complete times and duration,
// status codes, and number of bytes written.
//
// If the request context includes a trace ID, this will be included in the
// event data automatically.
//
// Each request will produce two log entries - one when the request is received,
// and another when the response has completed.
//
// See the Event and HTTP functions for additional information.
func Middleware(f http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req == nil {
			Event(nil, "nil request in middleware handler", FATAL)
			return
		}

		rc := &responseCapture{w, nil, 0}
		start := time.Now()
		Event(req.Context(), "http request received", HTTP(req, 0, 0, &start, nil))

		defer func() {
			end := time.Now()
			Event(req.Context(), "http request completed", HTTP(req, *rc.statusCode, rc.bytesWritten, &start, &end))
		}()

		f.ServeHTTP(rc, req)
	})
}

type responseCapture struct {
	http.ResponseWriter
	statusCode   *int
	bytesWritten int64
}

func (r *responseCapture) WriteHeader(status int) {
	r.statusCode = &status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseCapture) Write(b []byte) (n int, err error) {
	if r.statusCode == nil {
		s := 200
		r.statusCode = &s
	}
	n, err = r.ResponseWriter.Write(b)
	r.bytesWritten += int64(n)
	return
}

func (r *responseCapture) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *responseCapture) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := r.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errors.New("log: response does not implement http.Hijacker")
}
