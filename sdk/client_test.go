package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/health"
	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
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
	versionID            = "1"
	accessToken          = "myservicetoken"
	etag                 = "example-etag"
)

var ctx = context.Background()

var headers = Headers{
	CollectionID:         collectionID,
	DownloadServiceToken: downloadServiceToken,
	AccessToken:          accessToken,
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

func TestClientDoAuthenticatedPutRequest(t *testing.T) {
	mockHTTPClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, "", nil})
	client := newDatasetAPIHealthcheckClient(t, mockHTTPClient)

	Convey("Succeeds with valid values", t, func() {
		uri, _ := url.Parse("https://not-a-real-domain-this-is-a-test.com/target-path")
		payload := []byte(`{"testing_key":"testing_value"}`)
		resp, err := client.doAuthenticatedPutRequest(context.Background(), headers, uri, payload)

		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
	})
}

func TestClientDoAuthenticatedPutRequestWithETag(t *testing.T) {
	mockHTTPClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, "", nil})
	client := newDatasetAPIHealthcheckClient(t, mockHTTPClient)

	Convey("Succeeds with valid values", t, func() {
		uri, _ := url.Parse("https://testing.this.com/a-test")
		payload := []byte(`{"testing_key":"testing_value"}`)
		resp, err := client.DoAuthenticatedPutRequestWithEtag(context.Background(), headers, uri, payload, "123456")

		So(err, ShouldBeNil)
		So(resp, ShouldNotBeNil)
	})
}

func TestClientDoAuthenticatedPostRequest(t *testing.T) {
	Convey("Given a mocked dataset API client", t, func() {
		expectedResponseBody := map[string]string{"message": "success"}
		mockHTTPClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, expectedResponseBody, nil})
		client := newDatasetAPIHealthcheckClient(t, mockHTTPClient)

		Convey("When DoAuthenticatedPostRequest is called", func() {
			uri, err := url.Parse("https://domain.com/target-path")
			So(err, ShouldBeNil)

			payload := []byte(`{"testing_key":"testing_value"}`)
			resp, err := client.doAuthenticatedPostRequest(context.Background(), headers, uri, payload)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("And the response is returned with the expected body", func() {
				So(resp, ShouldNotBeNil)

				var respBody map[string]string
				err = json.NewDecoder(resp.Body).Decode(&respBody)
				So(err, ShouldBeNil)
				So(respBody, ShouldResemble, expectedResponseBody)
			})
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
			headers.add(&request)
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
		headers.add(&request)
		Convey("Test that Add() method updates `DownloadServiceHeaderKey` key with the correct value", func() {
			So(request.Header, ShouldContainKey, dpNetRequest.DownloadServiceHeaderKey)
			So(request.Header.Get(dpNetRequest.DownloadServiceHeaderKey), ShouldEqual, downloadServiceToken)
		})
		Convey("Test that Add() method doesn't update other keys", func() {
			So(request.Header, ShouldNotContainKey, dpNetRequest.AuthHeaderKey)
			So(request.Header, ShouldNotContainKey, dpNetRequest.CollectionIDHeaderKey)
		})
	})
	Convey("If Headers struct contains value for `CollectionID`", t, func() {
		headers := Headers{
			CollectionID: collectionID,
		}
		request := http.Request{
			Header: http.Header{},
		}
		headers.add(&request)
		Convey("Test that Add() method updates `CollectionIDHeaderKey` key with the correct value", func() {
			So(request.Header, ShouldContainKey, dpNetRequest.CollectionIDHeaderKey)
			So(request.Header.Get(dpNetRequest.CollectionIDHeaderKey), ShouldEqual, collectionID)
		})
		Convey("Test that Add() method doesn't update other keys", func() {
			So(request.Header, ShouldNotContainKey, dpNetRequest.AuthHeaderKey)
			So(request.Header, ShouldNotContainKey, dpNetRequest.DownloadServiceHeaderKey)
		})
	})
	Convey("If Headers struct contains value for `ServiceToken`", t, func() {
		headers := Headers{
			AccessToken: serviceToken,
		}
		request := http.Request{
			Header: http.Header{},
		}
		headers.add(&request)
		Convey("Test that Add() method updates `AuthHeaderKey` key with the correct value", func() {
			So(request.Header, ShouldContainKey, dpNetRequest.AuthHeaderKey)
			// Full value for `AuthHeaderKey` is "Bearer <serviceToken>"
			So(request.Header.Get(dpNetRequest.AuthHeaderKey), ShouldContainSubstring, serviceToken)
		})
		Convey("Test that Add() method doesn't update other keys", func() {
			So(request.Header, ShouldNotContainKey, dpNetRequest.CollectionIDHeaderKey)
			So(request.Header, ShouldNotContainKey, dpNetRequest.DownloadServiceHeaderKey)
		})
	})
	Convey("If Headers struct contains value for `UserAccessToken`", t, func() {
		headers := Headers{
			AccessToken: userAccessToken,
		}
		request := http.Request{
			Header: http.Header{},
		}
		headers.add(&request)
		Convey("Test that Add() method updates `Authorization` key with the correct value", func() {
			So(request.Header.Get(dpNetRequest.AuthHeaderKey), ShouldContainSubstring, userAccessToken)
		})
		Convey("Test that Add() method doesn't update other keys", func() {
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

// Tests for the `unmarshalResponseBodyExpectingStringError` function
func TestUnmarshalResponseBodyExpectingStringError(t *testing.T) {
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
			err := unmarshalResponseBodyExpectingStringError(mockResponse, &target)
			So(err, ShouldBeNil)
			So(target, ShouldResemble, requestedData)
		})
	})
	Convey("If response status code is not 404 (StatusNotFound)", t, func() {
		responseErr := errors.New("not found")
		responseJSON, _ := json.Marshal(responseErr.Error())
		mockResponse := &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(bytes.NewBuffer(responseJSON)),
		}
		target := mockTarget{}
		Convey("Test error is raised", func() {
			err := unmarshalResponseBodyExpectingStringError(mockResponse, &target)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, responseErr.Error())
		})
	})
}

// Tests for the `unmarshalResponseBodyExpectingErrorResponse` function
func TestUnmarshalResponseBodyExpectingErrorResponse(t *testing.T) {
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
			err := unmarshalResponseBodyExpectingErrorResponse(mockResponse, &target)
			So(err, ShouldBeNil)
			So(target, ShouldResemble, requestedData)
		})
	})

	Convey("If response status code is not 404 (StatusNotFound)", t, func() {
		responseErr := models.ErrorResponse{
			Errors: []models.Error{
				{
					Cause:       errs.ErrDatasetNotFound,
					Code:        "dataset_not_found",
					Description: "Dataset not found",
				},
			},
		}
		responseJSON, _ := json.Marshal(responseErr)
		mockResponse := &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(bytes.NewBuffer(responseJSON)),
		}
		target := mockTarget{}
		Convey("Test error is raised with the correct error message", func() {
			err := unmarshalResponseBodyExpectingErrorResponse(mockResponse, &target)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "Dataset not found")
		})
	})
}

type errorReader struct{}

func (e errorReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("mock read error")
}

func TestUnmarshalErrorResponse(t *testing.T) {
	Convey("Given a valid ErrorResponse body", t, func() {
		errorResponse := models.ErrorResponse{
			// cause field is not included as it is an ignored JSON field
			Errors: []models.Error{
				{
					Code:        models.InternalError,
					Description: models.InternalErrorDescription,
				},
			},
		}
		responseJSON, err := json.Marshal(errorResponse)
		So(err, ShouldBeNil)
		mockResponseBody := io.NopCloser(bytes.NewBuffer(responseJSON))

		Convey("When unmarshalErrorResponse is called", func() {
			result, err := unmarshalErrorResponse(mockResponseBody)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("And the expected ErrorResponse is returned", func() {
				So(result, ShouldResemble, &errorResponse)
			})
		})
	})

	Convey("Given an invalid ErrorResponse body", t, func() {
		body := `invalid json`
		mockResponseBody := io.NopCloser(strings.NewReader(body))

		Convey("When unmarshalErrorResponse is called", func() {
			result, err := unmarshalErrorResponse(mockResponseBody)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
			})

			Convey("And the result is nil", func() {
				So(result, ShouldBeNil)
			})
		})
	})

	Convey("Given a nil body", t, func() {
		Convey("When unmarshalErrorResponse is called", func() {
			result, err := unmarshalErrorResponse(nil)

			Convey("Then an error is returned", func() {
				So(err, ShouldEqual, errors.New("response body is nil"))
			})

			Convey("And the result is nil", func() {
				So(result, ShouldBeNil)
			})
		})
	})
}

func TestGetStringResponseBody(t *testing.T) {
	Convey("It succeeds when", t, func() {
		Convey("Body exists and is not empty", func() {
			mockResponseBody := "Test message"
			mockResponse := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(mockResponseBody)),
			}

			result, err := getStringResponseBody(mockResponse)

			So(err, ShouldBeNil)
			So(result, ShouldEqual, &mockResponseBody)
		})

		Convey("Body exists and is empty", func() {
			mockResponseBody := "Test message"
			mockResponse := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(mockResponseBody)),
			}

			result, err := getStringResponseBody(mockResponse)

			So(err, ShouldBeNil)
			So(result, ShouldEqual, &mockResponseBody)
		})

		Convey("Body exists and is JSON", func() {
			mockResponseBody := `{"test_key": "test_value"}`
			mockResponse := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader(mockResponseBody)),
			}

			result, err := getStringResponseBody(mockResponse)

			So(err, ShouldBeNil)
			So(result, ShouldEqual, &mockResponseBody)
		})
	})

	Convey("It errors when", t, func() {
		Convey("Error reading body", func() {
			mockResponse := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(errorReader{}),
			}

			result, err := getStringResponseBody(mockResponse)

			So(result, ShouldBeNil)
			So(err, ShouldNotBeNil)
		})
	})
}
