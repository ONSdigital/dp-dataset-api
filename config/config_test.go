package config

import (
	"os"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
)

func TestSpec(t *testing.T) {
	convey.Convey("Given an environment with no environment variables set", t, func() {
		os.Clearenv()
		cfg, err := Get()

		convey.Convey("When the config values are retrieved", func() {
			convey.Convey("Then there should be no error returned", func() {
				convey.So(err, convey.ShouldBeNil)
			})

			convey.Convey("The values should be set to the expected defaults", func() {
				convey.So(cfg.BindAddr, convey.ShouldEqual, ":22000")
				convey.So(cfg.KafkaAddr, convey.ShouldResemble, []string{"localhost:9092", "localhost:9093", "localhost:9094"})
				convey.So(cfg.KafkaConsumerMinBrokersHealthy, convey.ShouldEqual, 1)
				convey.So(cfg.KafkaProducerMinBrokersHealthy, convey.ShouldEqual, 2)
				convey.So(cfg.KafkaVersion, convey.ShouldEqual, "1.0.2")
				convey.So(cfg.KafkaSecProtocol, convey.ShouldEqual, "")
				convey.So(cfg.KafkaSecClientCert, convey.ShouldEqual, "")
				convey.So(cfg.KafkaSecClientKey, convey.ShouldEqual, "")
				convey.So(cfg.KafkaSecCACerts, convey.ShouldEqual, "")
				convey.So(cfg.KafkaSecSkipVerify, convey.ShouldBeFalse)
				convey.So(cfg.GenerateDownloadsTopic, convey.ShouldEqual, "filter-job-submitted")
				convey.So(cfg.CantabularExportStartTopic, convey.ShouldEqual, "cantabular-export-start")
				convey.So(cfg.DatasetAPIURL, convey.ShouldEqual, "http://localhost:22000")
				convey.So(cfg.CodeListAPIURL, convey.ShouldEqual, "http://localhost:22400")
				convey.So(cfg.DownloadServiceSecretKey, convey.ShouldEqual, "QB0108EZ-825D-412C-9B1D-41EF7747F462")
				convey.So(cfg.DownloadServiceURL, convey.ShouldEqual, "http://localhost:23600")
				convey.So(cfg.ImportAPIURL, convey.ShouldEqual, "http://localhost:21800")
				convey.So(cfg.WebsiteURL, convey.ShouldEqual, "http://localhost:20000")
				convey.So(cfg.ZebedeeURL, convey.ShouldEqual, "http://localhost:8082")
				convey.So(cfg.ServiceAuthToken, convey.ShouldEqual, "FD0108EA-825D-411C-9B1D-41EF7727F465")
				convey.So(cfg.GracefulShutdownTimeout, convey.ShouldEqual, 5*time.Second)
				convey.So(cfg.DisableGraphDBDependency, convey.ShouldEqual, false)
				convey.So(cfg.DefaultLimit, convey.ShouldEqual, 20)
				convey.So(cfg.DefaultOffset, convey.ShouldEqual, 0)
				convey.So(cfg.MaxRequestOptions, convey.ShouldEqual, 100)
				convey.So(cfg.EnablePermissionsAuth, convey.ShouldBeFalse)
				convey.So(cfg.HealthCheckCriticalTimeout, convey.ShouldEqual, 90*time.Second)
				convey.So(cfg.HealthCheckInterval, convey.ShouldEqual, 30*time.Second)
				convey.So(cfg.MongoConfig.ClusterEndpoint, convey.ShouldEqual, "localhost:27017")
				convey.So(cfg.MongoConfig.Database, convey.ShouldEqual, "datasets")
				convey.So(cfg.MongoConfig.Collections, convey.ShouldResemble, map[string]string{"DatasetsCollection": "datasets", "ContactsCollection": "contacts",
					"EditionsCollection": "editions", "InstanceCollection": "instances", "DimensionOptionsCollection": "dimension.options", "InstanceLockCollection": "instances_locks"})
				convey.So(cfg.MongoConfig.Username, convey.ShouldEqual, "")
				convey.So(cfg.MongoConfig.Password, convey.ShouldEqual, "")
				convey.So(cfg.MongoConfig.IsSSL, convey.ShouldEqual, false)
				convey.So(cfg.MongoConfig.QueryTimeout, convey.ShouldEqual, 15*time.Second)
				convey.So(cfg.MongoConfig.ConnectTimeout, convey.ShouldEqual, 5*time.Second)
				convey.So(cfg.MongoConfig.IsStrongReadConcernEnabled, convey.ShouldEqual, false)
				convey.So(cfg.MongoConfig.IsWriteConcernMajorityEnabled, convey.ShouldEqual, true)
				convey.So(cfg.MongoConfig.DatasetAPIURL, convey.ShouldEqual, "http://localhost:22000")
				convey.So(cfg.MongoConfig.CodeListAPIURL, convey.ShouldEqual, "http://localhost:22400")
			})
		})
	})
}
