package sdk

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	dpNetRequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/ONSdigital/log.go/v2/log"
	. "github.com/smartystreets/goconvey/convey"
)

// Tests for the `addCollectionIDHeader` function
func TestAddCollectionIDHeader(t *testing.T) {
	mockRequest := httptest.NewRequest("GET", "/", http.NoBody)

	Convey("If collectionID is empty string", t, func() {
		collectionID := ""
		Convey("Test `CollectionIDHeaderKey` field is not added to the request header", func() {
			addCollectionIDHeader(mockRequest, collectionID)
			So(mockRequest.Header.Values(dpNetRequest.CollectionIDHeaderKey), ShouldBeEmpty)
		})
	})

	Convey("If collectionID is a valid string", t, func() {
		collectionID := "1234"
		Convey("Test `CollectionIDHeaderKey` field is set to the request header", func() {
			addCollectionIDHeader(mockRequest, collectionID)
			So(mockRequest.Header.Values(dpNetRequest.CollectionIDHeaderKey), ShouldNotBeEmpty)
			So(mockRequest.Header.Get(dpNetRequest.CollectionIDHeaderKey), ShouldEqual, collectionID)
		})
	})
}

type mockReadCloser struct {
	raiseError bool
}

// Implemented just to keep compiler happy for mock object
func (m *mockReadCloser) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

// Returns an error if `raiseError` is `true`, otherwise `false`
func (m *mockReadCloser) Close() error {
	if m.raiseError {
		return errors.New("error closing body")
	}
	return nil
}

// Tests for `closeResponseBody` function
func TestCloseResponseBody(t *testing.T) {
	ctx := context.Background()
	// Create a buffer to capture log output for tests
	var buf bytes.Buffer
	var fbBuf bytes.Buffer
	log.SetDestination(&buf, &fbBuf)

	Convey("Test function runs without logging an error if body.Close() completes without error", t, func() {
		mockResponse := http.Response{Body: &mockReadCloser{raiseError: false}}

		closeResponseBody(ctx, &mockResponse)
		So(buf.String(), ShouldBeEmpty)
	})
	Convey("Test function logs an error if body.Close() returns an error", t, func() {
		mockResponse := http.Response{Body: &mockReadCloser{raiseError: true}}

		closeResponseBody(ctx, &mockResponse)
		So(buf.String(), ShouldContainSubstring, "error closing http response body")
	})
}
