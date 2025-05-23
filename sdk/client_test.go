package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/health"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	dphttp "github.com/ONSdigital/dp-net/v3/http"
	dpNetRequest "github.com/ONSdigital/dp-net/v3/request"
	"github.com/ONSdigital/log.go/v2/log"
	. "github.com/smartystreets/goconvey/convey"
)

// Used throughout the sdk tests
const (
	datasetAPIURL        = "http://localhost:22000"
	datasetID            = "1234"
	downloadServiceToken = "mydownloadservicetoken"
	collectionID         = "collection"
	editionID            = "my-edition"
	serviceToken         = "myservicetoken"
	userAccessToken      = "myuseraccesstoken"
)

var ctx = context.Background()

var headers = Headers{
	CollectionID:         collectionID,
	DownloadServiceToken: downloadServiceToken,
	ServiceToken:         serviceToken,
	UserAccessToken:      userAccessToken,
}

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

// Test the `Headers` struct and associated methods
func TestHeaders(t *testing.T) {
	downloadServiceToken := "mydownloadservicetoken"
	collectionID := "collection"
	serviceToken := "myservicetoken"
	userAccessToken := "myuseraccesstoken"

	Convey("If Headers struct is empty", t, func() {
		headers := Headers{}
		request := http.Request{
			Header: http.Header{},
		}
		Convey("Test that Add() method doesn't update request headers", func() {
			headers.Add(&request)
			So(request.Header, ShouldBeEmpty)
		})
	})
	Convey("If Headers struct contains value for `DownloadServiceToken`", t, func() {
		headers := Headers{
			DownloadServiceToken: downloadServiceToken,
		}
		request := http.Request{
			Header: http.Header{},
		}
		headers.Add(&request)
		Convey("Test that Add() method updates `DownloadServiceHeaderKey` key with the correct value", func() {
			So(request.Header, ShouldContainKey, dpNetRequest.DownloadServiceHeaderKey)
			So(request.Header.Get(dpNetRequest.DownloadServiceHeaderKey), ShouldEqual, downloadServiceToken)
		})
		Convey("Test that Add() method doesn't update other keys", func() {
			So(request.Header, ShouldNotContainKey, dpNetRequest.AuthHeaderKey)
			So(request.Header, ShouldNotContainKey, dpNetRequest.CollectionIDHeaderKey)
			So(request.Header, ShouldNotContainKey, dpNetRequest.FlorenceHeaderKey)
		})
	})
	Convey("If Headers struct contains value for `CollectionID`", t, func() {
		headers := Headers{
			CollectionID: collectionID,
		}
		request := http.Request{
			Header: http.Header{},
		}
		headers.Add(&request)
		Convey("Test that Add() method updates `CollectionIDHeaderKey` key with the correct value", func() {
			So(request.Header, ShouldContainKey, dpNetRequest.CollectionIDHeaderKey)
			So(request.Header.Get(dpNetRequest.CollectionIDHeaderKey), ShouldEqual, collectionID)
		})
		Convey("Test that Add() method doesn't update other keys", func() {
			So(request.Header, ShouldNotContainKey, dpNetRequest.AuthHeaderKey)
			So(request.Header, ShouldNotContainKey, dpNetRequest.DownloadServiceHeaderKey)
			So(request.Header, ShouldNotContainKey, dpNetRequest.FlorenceHeaderKey)
		})
	})
	Convey("If Headers struct contains value for `ServiceToken`", t, func() {
		headers := Headers{
			ServiceToken: serviceToken,
		}
		request := http.Request{
			Header: http.Header{},
		}
		headers.Add(&request)
		Convey("Test that Add() method updates `AuthHeaderKey` key with the correct value", func() {
			So(request.Header, ShouldContainKey, dpNetRequest.AuthHeaderKey)
			// Full value for `AuthHeaderKey` is "Bearer <serviceToken>"
			So(request.Header.Get(dpNetRequest.AuthHeaderKey), ShouldContainSubstring, serviceToken)
		})
		Convey("Test that Add() method doesn't update other keys", func() {
			So(request.Header, ShouldNotContainKey, dpNetRequest.CollectionIDHeaderKey)
			So(request.Header, ShouldNotContainKey, dpNetRequest.DownloadServiceHeaderKey)
			So(request.Header, ShouldNotContainKey, dpNetRequest.FlorenceHeaderKey)
		})
	})
	Convey("If Headers struct contains value for `UserAccessToken`", t, func() {
		headers := Headers{
			UserAccessToken: userAccessToken,
		}
		request := http.Request{
			Header: http.Header{},
		}
		headers.Add(&request)
		Convey("Test that Add() method updates `FlorenceHeaderKey` key with the correct value", func() {
			So(request.Header, ShouldContainKey, dpNetRequest.FlorenceHeaderKey)
			So(request.Header.Get(dpNetRequest.FlorenceHeaderKey), ShouldEqual, userAccessToken)
		})
		Convey("Test that Add() method doesn't update other keys", func() {
			So(request.Header, ShouldNotContainKey, dpNetRequest.AuthHeaderKey)
			So(request.Header, ShouldNotContainKey, dpNetRequest.CollectionIDHeaderKey)
			So(request.Header, ShouldNotContainKey, dpNetRequest.DownloadServiceHeaderKey)
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

type mockTarget struct {
	FieldOne string
	FieldTwo string
}

// Tests for the `unmarshalResponseBody` function
func TestUnmarshalResponseBody(t *testing.T) {
	Convey("If response status code is 200 (StatusOK)", t, func() {
		requestedData := mockTarget{
			FieldOne: "hello",
			FieldTwo: "test",
		}
		responseJSON, _ := json.Marshal(requestedData)
		mockResponse := &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewBuffer(responseJSON)),
		}
		target := mockTarget{}
		Convey("Test response body is unmarshaled to target", func() {
			err := unmarshalResponseBody(mockResponse, &target)
			So(err, ShouldBeNil)
			So(target, ShouldResemble, requestedData)
		})
	})
	Convey("If response status code is not 404 (StatusNotFound)", t, func() {
		responseErr := errors.New("Not found!")
		responseJSON, _ := json.Marshal(responseErr.Error())
		mockResponse := &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(bytes.NewBuffer(responseJSON)),
		}
		target := mockTarget{}
		Convey("Test error is raised", func() {
			err := unmarshalResponseBody(mockResponse, &target)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, responseErr.Error())
		})
	})
	Convey("If response status code is not 404 (Unauthorised)", t, func() {
		responseStr := "unauthorised access to API\n"
		// Response body is an example of what the api returns when returning an unauthorised response
		mockResponse := &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(bytes.NewBuffer([]byte(responseStr))),
		}
		target := mockTarget{}
		Convey("Test error is raised", func() {
			err := unmarshalResponseBody(mockResponse, &target)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, responseStr)
		})
	})
}
