package cloudflare

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNew(t *testing.T) {
	Convey("Given a valid Cloudflare config", t, func() {
		cfg := NewDefaultConfig()

		Convey("When New is called", func() {
			client, err := New(cfg)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("And the client is returned", func() {
				So(client, ShouldNotBeNil)
			})
		})
	})

	Convey("Given a nil Cloudflare config", t, func() {
		Convey("When New is called", func() {
			client, err := New(nil)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "configuration cannot be nil")
			})

			Convey("And the client is nil", func() {
				So(client, ShouldBeNil)
			})
		})
	})

	Convey("Given an invalid Cloudflare config with missing BaseURL", t, func() {
		cfg := NewDefaultConfig()
		cfg.BaseURL = ""

		Convey("When New is called", func() {
			client, err := New(cfg)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "base URL is required")
			})

			Convey("And the client is nil", func() {
				So(client, ShouldBeNil)
			})
		})
	})

	Convey("Given an invalid Cloudflare config with missing APIToken", t, func() {
		cfg := NewDefaultConfig()
		cfg.APIToken = ""

		Convey("When New is called", func() {
			client, err := New(cfg)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "API token is required")
			})

			Convey("And the client is nil", func() {
				So(client, ShouldBeNil)
			})
		})
	})

	Convey("Given an invalid Cloudflare config with missing ZoneID", t, func() {
		cfg := NewDefaultConfig()
		cfg.ZoneID = ""

		Convey("When New is called", func() {
			client, err := New(cfg)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "zone ID is required")
			})

			Convey("And the client is nil", func() {
				So(client, ShouldBeNil)
			})
		})
	})
}
