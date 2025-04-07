package sdk

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	datasetAPIURL = "http://wwww.test.com"
)

func newDatasetAPIClient(_ *testing.T) *Client {
	return New(datasetAPIURL)
}

// func newDatasetAPIHealthcheckClient(_ *testing.T, httpClient *dphttp.ClienterMock) *Client {
// 	healthClient := health.NewClientWithClienter(service, testHost, httpClient)
// 	return NewWithHealthClient(healthClient)
// }

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

// func TestHealthCheckerClient(t *testing.T) {

// }
