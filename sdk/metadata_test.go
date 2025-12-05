package sdk

import (
	"net/http"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_PutMetadata(t *testing.T) {
	Convey("Given a valid metadata record", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, "", nil})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

		Convey("when put metadata is called", func() {
			metadata := EditableMetadata{Description: "test dataset", Title: "testing 1234"}
			err := datasetAPIClient.PutMetadata(ctx, headers, "666", "test", "1", metadata, "1234")

			Convey("then no error is returned", func() {
				So(err, ShouldBeNil)
			})
		})
	})

	Convey("Given no auth token has been configured so the request is unauthorized", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusUnauthorized, "", nil})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

		Convey("when put instance is called", func() {
			metadata := EditableMetadata{Description: "test dataset", Title: "testing 1234"}
			err := datasetAPIClient.PutMetadata(ctx, headers, "666", "test", "1", metadata, "1234")

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "did not receive success response. received status 401")
			})
		})
	})
}
