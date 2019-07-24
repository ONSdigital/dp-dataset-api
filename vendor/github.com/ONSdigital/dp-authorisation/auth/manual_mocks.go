package auth

import (
	"io"
	"net/http"
)

type HandlerMock struct {
	count int
}

// ReadCloser is a mocked impl of an io.ReadCloser
type readCloserMock struct {
	GetEntityFunc func() ([]byte, error)
	done          bool
}

func (h *HandlerMock) handleFunc(http.ResponseWriter, *http.Request) {
	h.count += 1
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

