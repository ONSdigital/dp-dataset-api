package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/health"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
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
