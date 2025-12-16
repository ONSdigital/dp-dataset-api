package sdk

import (
	"net/http"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_AddHeaders(t *testing.T) {

	Convey("When a request is made to /datasets", t, func() {

		req, err := http.NewRequest(http.MethodGet, "/datasets", nil)

		Convey("With a user token including the Bearer prefix", func() {

			So(err, ShouldBeNil)

			h := Headers{AccessToken: "Bearer 1234555555"}
			h.add(req)

			Convey("Then it is removed", func() {
				So(h.AccessToken, ShouldEqual, "1234555555")
			})
		})

		Convey("With a user token with no bearer prefix", func() {

			So(err, ShouldBeNil)

			h := Headers{AccessToken: "1234555555"}
			h.add(req)

			Convey("Then the token is not changed", func() {
				So(h.AccessToken, ShouldEqual, "1234555555")
			})
		})
	})
}
