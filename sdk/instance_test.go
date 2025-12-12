package sdk

import (
	"net/http"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_PutInstance(t *testing.T) {
	Convey("Given a valid instance", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusOK, "", map[string]string{"ETag": "1234"}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

		Convey("when put instance is called", func() {
			instance := UpdateInstance{ID: "1234"}
			str, err := datasetAPIClient.PutInstance(ctx, headers, "666", instance, "1234")

			Convey("then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("and the returned etag matches", func() {
				So(str, ShouldEqual, "1234")
			})
		})
	})

	Convey("Given no auth token has been configured so the request is unauthorized", t, func() {
		httpClient := createHTTPClientMock(MockedHTTPResponse{http.StatusUnauthorized, "", map[string]string{"ETag": "1234"}})
		datasetAPIClient := newDatasetAPIHealthcheckClient(t, httpClient)

		Convey("when put instance is called", func() {
			instance := UpdateInstance{ID: "1234"}
			str, err := datasetAPIClient.PutInstance(ctx, headers, "666", instance, "1234")

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "did not receive success response. received status 401")
			})

			Convey("and the etag should be empty", func() {
				So(str, ShouldEqual, "")
			})
		})
	})
}
