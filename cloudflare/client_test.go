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
				So(err, ShouldEqual, errNilConfig)
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
				So(err, ShouldEqual, errMissingBaseURL)
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
				So(err, ShouldEqual, errMissingAPIToken)
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
				So(err, ShouldEqual, errMissingZoneID)
			})

			Convey("And the client is nil", func() {
				So(client, ShouldBeNil)
			})
		})
	})

	Convey("Given an invalid Cloudflare config with an invalid Timeout", t, func() {
		cfg := NewDefaultConfig()
		cfg.Timeout = 0

		Convey("When New is called", func() {
			client, err := New(cfg)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, errInvalidTimeout)
			})

			Convey("And the client is nil", func() {
				So(client, ShouldBeNil)
			})
		})
	})
}

func TestGetTimeout(t *testing.T) {
	Convey("Given a Cloudflare client with a specific timeout", t, func() {
		cfg := NewDefaultConfig()
		client, err := New(cfg)
		So(err, ShouldBeNil)

		Convey("When GetTimeout is called", func() {
			timeout := client.GetTimeout()

			Convey("Then the returned timeout matches the configured timeout", func() {
				So(timeout, ShouldEqual, cfg.Timeout)
			})
		})
	})
}
