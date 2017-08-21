package config

import (
	"testing"

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
				So(cfg.PostgresDatasetsURL, ShouldEqual, "user=dp dbname=Datasets sslmode=disable")
				So(cfg.SecretKey, ShouldEqual, "FD0108EA-825D-411C-9B1D-41EF7727F465")
			})
		})
	})
}
