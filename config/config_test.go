package config

import (
	"os"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSpec(t *testing.T) {
	Convey("Given an environment with no environment variables set", t, func() {
		os.Clearenv()
		cfg, err := Get()

		Convey("When the config values are retrieved", func() {
			Convey("Then there should be no error returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("The values should be set to the expected defaults", func() {
				So(cfg.BindAddr, ShouldEqual, ":22000")
				So(cfg.KafkaAddr, ShouldResemble, []string{"localhost:9092", "localhost:9093", "localhost:9094"})
				So(cfg.KafkaConsumerMinBrokersHealthy, ShouldEqual, 1)
				So(cfg.KafkaProducerMinBrokersHealthy, ShouldEqual, 2)
				So(cfg.KafkaVersion, ShouldEqual, "1.0.2")
				So(cfg.KafkaSecProtocol, ShouldEqual, "")
				So(cfg.KafkaSecClientCert, ShouldEqual, "")
				So(cfg.KafkaSecClientKey, ShouldEqual, "")
				So(cfg.KafkaSecCACerts, ShouldEqual, "")
				So(cfg.KafkaSecSkipVerify, ShouldBeFalse)
				So(cfg.GenerateDownloadsTopic, ShouldEqual, "filter-job-submitted")
				So(cfg.CantabularExportStartTopic, ShouldEqual, "cantabular-export-start")
				So(cfg.DatasetAPIURL, ShouldEqual, "http://localhost:22000")
				So(cfg.CodeListAPIURL, ShouldEqual, "http://localhost:22400")
				So(cfg.DownloadServiceSecretKey, ShouldEqual, "QB0108EZ-825D-412C-9B1D-41EF7747F462")
				So(cfg.DownloadServiceURL, ShouldEqual, "http://localhost:23600")
				So(cfg.ImportAPIURL, ShouldEqual, "http://localhost:21800")
				So(cfg.WebsiteURL, ShouldEqual, "http://localhost:20000")
				So(cfg.ZebedeeURL, ShouldEqual, "http://localhost:8082")
				So(cfg.ServiceAuthToken, ShouldEqual, "FD0108EA-825D-411C-9B1D-41EF7727F465")
				So(cfg.GracefulShutdownTimeout, ShouldEqual, 5*time.Second)
				So(cfg.DisableGraphDBDependency, ShouldEqual, false)
				So(cfg.DefaultLimit, ShouldEqual, 20)
				So(cfg.DefaultOffset, ShouldEqual, 0)
				So(cfg.MaxRequestOptions, ShouldEqual, 100)
				So(cfg.EnablePermissionsAuth, ShouldBeFalse)
				So(cfg.EnableURLRewriting, ShouldBeFalse)
				So(cfg.HealthCheckCriticalTimeout, ShouldEqual, 90*time.Second)
				So(cfg.HealthCheckInterval, ShouldEqual, 30*time.Second)
				So(cfg.ClusterEndpoint, ShouldEqual, "localhost:27017")
				So(cfg.Database, ShouldEqual, "datasets")
				So(cfg.Collections, ShouldResemble, map[string]string{"DatasetsCollection": "datasets", "ContactsCollection": "contacts",
					"EditionsCollection": "editions", "InstanceCollection": "instances", "DimensionOptionsCollection": "dimension.options", "InstanceLockCollection": "instances_locks", "VersionsCollection": "versions"})
				So(cfg.Username, ShouldEqual, "")
				So(cfg.Password, ShouldEqual, "")
				So(cfg.IsSSL, ShouldEqual, false)
				So(cfg.QueryTimeout, ShouldEqual, 15*time.Second)
				So(cfg.ConnectTimeout, ShouldEqual, 5*time.Second)
				So(cfg.IsStrongReadConcernEnabled, ShouldEqual, false)
				So(cfg.IsWriteConcernMajorityEnabled, ShouldEqual, true)
				So(cfg.DatasetAPIURL, ShouldEqual, "http://localhost:22000")
				So(cfg.CodeListAPIURL, ShouldEqual, "http://localhost:22400")
				So(cfg.CloudflareAPIToken, ShouldEqual, "test-token")
				So(cfg.CloudflareZoneID, ShouldEqual, "9f1eec58caedd8e902395a065c120073")
			})
		})
	})
}
