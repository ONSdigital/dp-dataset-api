package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/health"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	dpNetRequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/ONSdigital/log.go/v2/log"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	datasetAPIURL = "http://localhost:25700"
)

type MockedHTTPResponse struct {
	StatusCode int
	Body       interface{}
	Headers    map[string]string
}

func newDatasetAPIClient(_ *testing.T) *Client {
	return New(datasetAPIURL)
}

func createHTTPClientMock(mockedHTTPResponse ...MockedHTTPResponse) *dphttp.ClienterMock {
	numCall := 0
	return &dphttp.ClienterMock{
		DoFunc: func(ctx context.Context, req *http.Request) (*http.Response, error) {
			body, _ := json.Marshal(mockedHTTPResponse[numCall].Body)
			resp := &http.Response{
				StatusCode: mockedHTTPResponse[numCall].StatusCode,
				Body:       io.NopCloser(bytes.NewReader(body)),
				Header:     http.Header{},
			}
			for hKey, hVal := range mockedHTTPResponse[numCall].Headers {
				resp.Header.Set(hKey, hVal)
			}
			numCall++
			return resp, nil
		},
		SetPathsWithNoRetriesFunc: func(paths []string) {},
		GetPathsWithNoRetriesFunc: func() []string {
			return []string{"/healthcheck"}
		},
	}
}

func newDatasetAPIHealthcheckClient(_ *testing.T, httpClient *dphttp.ClienterMock) *Client {
	healthClient := health.NewClientWithClienter(service, datasetAPIURL, httpClient)
	return NewWithHealthClient(healthClient)
}

// Tests for the `New()` sdk client method
func TestClient(t *testing.T) {
	client := newDatasetAPIClient(t)

	Convey("Test client URL() method returns correct url", t, func() {
		So(client.URL(), ShouldEqual, datasetAPIURL)
	})

	Convey("Test client Health() method returns correct health client", t, func() {
		So(client.Health(), ShouldNotBeNil)
		So(client.hcCli.Name, ShouldEqual, service)
		So(client.hcCli.URL, ShouldEqual, datasetAPIURL)
	})
}

// Tests for the `NewWithHealthClient()` sdk client method
func TestHealthCheckerClient(t *testing.T) {
	ctx := context.Background()
	initialStateCheck := health.CreateCheckState(service)

	Convey("If http client returns 200 OK response", t, func() {
		mockHTTPClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, "", nil})
		client := newDatasetAPIHealthcheckClient(t, mockHTTPClient)

		Convey("Test client URL() method returns correct url", func() {
			So(client.URL(), ShouldEqual, datasetAPIURL)
		})

		Convey("Test client Health() method returns correct health client", func() {
			So(client.Health(), ShouldNotBeNil)
			So(client.hcCli.Name, ShouldEqual, service)
			So(client.hcCli.URL, ShouldEqual, datasetAPIURL)
		})

		Convey("Test client Checker() method returns expected check", func() {
			err := client.Checker(ctx, &initialStateCheck)
			So(err, ShouldBeNil)
			So(initialStateCheck.Name(), ShouldEqual, service)
			So(initialStateCheck.Status(), ShouldEqual, healthcheck.StatusOK)
		})
	})
}

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
	var buf bytes.Buffer
	var fbBuf bytes.Buffer
	log.SetDestination(&buf, &fbBuf)

	Convey("Test function runs without logging an error if body.Close() completes without error", t, func() {
		mockResponse := http.Response{Body: &mockReadCloser{raiseError: false}}

		closeResponseBody(ctx, &mockResponse)
		So(buf.String(), ShouldBeEmpty)
	})
	Convey("Test function logs an error if body.Close() returns an error", t, func() {
		// Create a buffer to capture log output for this test
		var buf bytes.Buffer
		var fbBuf bytes.Buffer
		log.SetDestination(&buf, &fbBuf)

		mockResponse := http.Response{Body: &mockReadCloser{raiseError: true}}

		closeResponseBody(ctx, &mockResponse)
		So(buf.String(), ShouldContainSubstring, "error closing http response body")
	})
}
