package config

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSpec(t *testing.T) {
	Convey("Given an environment with no environment variables set", t, func() {
		cfg, err := Get()

		Convey("When the config values are retrieved", func() {

			Convey("There should be no error returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("The values should be set to the expected defaults", func() {
				So(cfg.BindAddr, ShouldEqual, ":22000")
				So(cfg.DatasetAPIURL, ShouldEqual, "http://localhost:22000")
				So(cfg.SecretKey, ShouldEqual, "FD0108EA-825D-411C-9B1D-41EF7727F465")
				So(cfg.ShutdownTimeout, ShouldEqual, 5*time.Second)
				So(cfg.MongoConfig.BindAddr, ShouldEqual, "localhost:27017")
				So(cfg.MongoConfig.Collection, ShouldEqual, "datasets")
				So(cfg.MongoConfig.Database, ShouldEqual, "datasets")
			})
		})
	})
}
