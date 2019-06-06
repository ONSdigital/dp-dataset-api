package authorisation

import (
	"context"
	"io"
	"net/http"
)

type responseWriterMock struct {
	HeaderFunc       func() http.Header
	WriteCalls       []string
	WriteFunc        func([]byte) (int, error)
	WriteHeaderCalls []int
	WriteHeaderFunc  func(statusCode int)
}

// ReadCloser is a mocked impl of an io.ReadCloser
type readCloserMock struct {
	GetEntityFunc func() ([]byte, error)
	done          bool
}

type httpClientMock struct {
	calls  []*http.Request
	DoFunc func() (*http.Response, error)
}

func (w *responseWriterMock) Header() http.Header {
	return w.HeaderFunc()
}

func (w *responseWriterMock) Write(b []byte) (int, error) {
	w.WriteCalls = append(w.WriteCalls, string(b))
	return w.WriteFunc(b)
}

func (w *responseWriterMock) WriteHeader(status int) {
	w.WriteHeaderCalls = append(w.WriteHeaderCalls, status)
	w.WriteHeaderFunc(status)
}

func (rc *readCloserMock) Read(p []byte) (n int, err error) {
	if rc.done {
		return 0, io.EOF
	}

	b, err := rc.GetEntityFunc()
	if err != nil {
		return 0, err
	}

	for i, b := range b {
		p[i] = b
	}
	rc.done = true
	return len(b), nil
}

func (rc *readCloserMock) Close() error {
	return nil
}

func (m *httpClientMock) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	m.calls = append(m.calls, req)
	return m.DoFunc()
}
