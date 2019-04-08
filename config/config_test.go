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

			Convey("Then there should be no error returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("The values should be set to the expected defaults", func() {
				So(cfg.BindAddr, ShouldEqual, ":22000")
				So(cfg.FeatureDetachDataset, ShouldEqual, false)
				So(cfg.KafkaAddr, ShouldResemble, []string{"localhost:9092"})
				So(cfg.AuditEventsTopic, ShouldEqual, "audit-events")
				So(cfg.GenerateDownloadsTopic, ShouldEqual, "filter-job-submitted")
				So(cfg.DatasetAPIURL, ShouldEqual, "http://localhost:22000")
				So(cfg.CodeListAPIURL, ShouldEqual, "http://localhost:22400")
				So(cfg.DownloadServiceSecretKey, ShouldEqual, "QB0108EZ-825D-412C-9B1D-41EF7747F462")
				So(cfg.WebsiteURL, ShouldEqual, "http://localhost:20000")
				So(cfg.ZebedeeURL, ShouldEqual, "http://localhost:8082")
				So(cfg.AuditEventsTopic, ShouldEqual, "audit-events")
				So(cfg.ServiceAuthToken, ShouldEqual, "FD0108EA-825D-411C-9B1D-41EF7727F465")
				So(cfg.GracefulShutdownTimeout, ShouldEqual, 5*time.Second)
				So(cfg.MongoConfig.BindAddr, ShouldEqual, "localhost:27017")
				So(cfg.MongoConfig.Collection, ShouldEqual, "datasets")
				So(cfg.MongoConfig.Database, ShouldEqual, "datasets")
			})
		})
	})
}
