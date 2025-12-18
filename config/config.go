package config

import (
	"encoding/json"
	"time"

	"github.com/kelseyhightower/envconfig"

	"github.com/ONSdigital/dp-authorisation/v2/authorisation"
	"github.com/ONSdigital/dp-dataset-api/cloudflare"
	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
)

type MongoConfig struct {
	mongodriver.MongoDriverConfig

	CodeListAPIURL string `envconfig:"CODE_LIST_API_URL"`
	DatasetAPIURL  string `envconfig:"DATASET_API_URL"`
}

// Configuration structure which hold information for configuring the import API
type Configuration struct {
	BindAddr                       string        `envconfig:"BIND_ADDR"`
	KafkaAddr                      []string      `envconfig:"KAFKA_ADDR"                       json:"-"`
	KafkaConsumerMinBrokersHealthy int           `envconfig:"KAFKA_CONSUMER_MIN_BROKERS_HEALTHY"`
	KafkaProducerMinBrokersHealthy int           `envconfig:"KAFKA_PRODUCER_MIN_BROKERS_HEALTHY"`
	KafkaSecProtocol               string        `envconfig:"KAFKA_SEC_PROTO"`
	KafkaSecCACerts                string        `envconfig:"KAFKA_SEC_CA_CERTS"`
	KafkaSecClientCert             string        `envconfig:"KAFKA_SEC_CLIENT_CERT"`
	KafkaSecClientKey              string        `envconfig:"KAFKA_SEC_CLIENT_KEY"             json:"-"`
	KafkaSecSkipVerify             bool          `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
	GenerateDownloadsTopic         string        `envconfig:"GENERATE_DOWNLOADS_TOPIC"`
	CantabularExportStartTopic     string        `envconfig:"CANTABULAR_EXPORT_START"`
	SearchContentUpdatedTopic      string        `envconfig:"SEARCH_CONTENT_UPDATED_TOPIC"`
	APIRouterPublicURL             string        `envconfig:"API_ROUTER_PUBLIC_URL"`
	CodeListAPIURL                 string        `envconfig:"CODE_LIST_API_URL"`
	DatasetAPIURL                  string        `envconfig:"DATASET_API_URL"`
	FilesAPIURL                    string        `envconfig:"FILES_API_URL"`
	DownloadServiceURL             string        `envconfig:"DOWNLOAD_SERVICE_URL"`
	ImportAPIURL                   string        `envconfig:"IMPORT_API_URL"`
	WebsiteURL                     string        `envconfig:"WEBSITE_URL"`
	ZebedeeURL                     string        `envconfig:"ZEBEDEE_URL"`
	DownloadServiceSecretKey       string        `envconfig:"DOWNLOAD_SERVICE_SECRET_KEY"      json:"-"`
	ServiceAuthToken               string        `envconfig:"SERVICE_AUTH_TOKEN"               json:"-"`
	GracefulShutdownTimeout        time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	HealthCheckInterval            time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	HealthCheckCriticalTimeout     time.Duration `envconfig:"HEALTHCHECK_CRITICAL_TIMEOUT"`
	EnablePrivateEndpoints         bool          `envconfig:"ENABLE_PRIVATE_ENDPOINTS"`
	EnableDetachDataset            bool          `envconfig:"ENABLE_DETACH_DATASET"`
	EnableDeleteStaticVersion      bool          `envconfig:"ENABLE_DELETE_STATIC_VERSION"`
	EnablePermissionsAuth          bool          `envconfig:"ENABLE_PERMISSIONS_AUTH"`
	EnableObservationEndpoint      bool          `envconfig:"ENABLE_OBSERVATION_ENDPOINT"`
	EnableURLRewriting             bool          `envconfig:"ENABLE_URL_REWRITING"`
	DisableGraphDBDependency       bool          `envconfig:"DISABLE_GRAPH_DB_DEPENDENCY"`
	KafkaVersion                   string        `envconfig:"KAFKA_VERSION"`
	DefaultMaxLimit                int           `envconfig:"DEFAULT_MAXIMUM_LIMIT"`
	DefaultLimit                   int           `envconfig:"DEFAULT_LIMIT"`
	DefaultOffset                  int           `envconfig:"DEFAULT_OFFSET"`
	MaxRequestOptions              int           `envconfig:"MAX_REQUEST_OPTIONS"`
	EncryptionDisabled             bool          `envconfig:"ENCRYPTION_DISABLED"`
	ComponentTestUseLogFile        bool          `envconfig:"COMPONENT_TEST_USE_LOG_FILE"`
	OTExporterOTLPEndpoint         string        `envconfig:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	OTServiceName                  string        `envconfig:"OTEL_SERVICE_NAME"`
	OTBatchTimeout                 time.Duration `envconfig:"OTEL_BATCH_TIMEOUT"`
	OtelEnabled                    bool          `envconfig:"OTEL_ENABLED"`
	MongoConfig
	AuthConfig        *authorisation.Config
	CloudflareEnabled bool `envconfig:"CLOUDFLARE_ENABLED"`
	CloudflareConfig  *cloudflare.Config
}

var cfg *Configuration

const (
	DatasetsCollection         = "DatasetsCollection"
	ContactsCollection         = "ContactsCollection"
	EditionsCollection         = "EditionsCollection"
	InstanceCollection         = "InstanceCollection"
	DimensionOptionsCollection = "DimensionOptionsCollection"
	InstanceLockCollection     = "InstanceLockCollection"
	VersionsCollection         = "VersionsCollection"
)

// Get the application and returns the configuration structure, and initialises with default values.
func Get() (*Configuration, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Configuration{
		BindAddr:                       ":22000",
		KafkaAddr:                      []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		KafkaConsumerMinBrokersHealthy: 1,
		KafkaProducerMinBrokersHealthy: 2,
		GenerateDownloadsTopic:         "filter-job-submitted",
		CantabularExportStartTopic:     "cantabular-export-start",
		SearchContentUpdatedTopic:      "search-content-updated",
		APIRouterPublicURL:             "http://localhost:23200/v1",
		CodeListAPIURL:                 "http://localhost:22400",
		DatasetAPIURL:                  "http://localhost:22000",
		FilesAPIURL:                    "http://localhost:26900",
		DownloadServiceURL:             "http://localhost:23600",
		ImportAPIURL:                   "http://localhost:21800",
		WebsiteURL:                     "http://localhost:20000",
		ZebedeeURL:                     "http://localhost:8082",
		ServiceAuthToken:               "FD0108EA-825D-411C-9B1D-41EF7727F465",
		DownloadServiceSecretKey:       "QB0108EZ-825D-412C-9B1D-41EF7747F462",
		GracefulShutdownTimeout:        5 * time.Second,
		HealthCheckInterval:            30 * time.Second,
		HealthCheckCriticalTimeout:     90 * time.Second,
		OTExporterOTLPEndpoint:         "localhost:4317",
		OTServiceName:                  "dp-dataset-api",
		OTBatchTimeout:                 5 * time.Second,
		OtelEnabled:                    false,
		EnablePrivateEndpoints:         false,
		EnableDetachDataset:            false,
		EnableDeleteStaticVersion:      false,
		EnableObservationEndpoint:      true,
		EnableURLRewriting:             false,
		DisableGraphDBDependency:       false,
		KafkaVersion:                   "1.0.2",
		DefaultMaxLimit:                1000,
		DefaultLimit:                   20,
		DefaultOffset:                  0,
		MaxRequestOptions:              100, // Maximum number of options acceptable in an incoming Patch request. Compromise between one option per call (inefficient) and an order of 100k options per call, for census data (memory and computationally expensive)
		MongoConfig: MongoConfig{
			MongoDriverConfig: mongodriver.MongoDriverConfig{
				ClusterEndpoint:               "localhost:27017",
				Username:                      "",
				Password:                      "",
				Database:                      "datasets",
				Collections:                   map[string]string{DatasetsCollection: "datasets", ContactsCollection: "contacts", EditionsCollection: "editions", InstanceCollection: "instances", DimensionOptionsCollection: "dimension.options", InstanceLockCollection: "instances_locks", VersionsCollection: "versions"},
				ReplicaSet:                    "",
				IsStrongReadConcernEnabled:    false,
				IsWriteConcernMajorityEnabled: true,
				ConnectTimeout:                5 * time.Second,
				QueryTimeout:                  15 * time.Second,
				TLSConnectionConfig: mongodriver.TLSConnectionConfig{
					IsSSL: false,
				},
			},
			CodeListAPIURL: "http://localhost:22400",
			DatasetAPIURL:  "http://localhost:22000",
		},
		ComponentTestUseLogFile: false,
		AuthConfig:              authorisation.NewDefaultConfig(),
		CloudflareEnabled:       false,
		CloudflareConfig:        cloudflare.NewDefaultConfig(),
	}

	return cfg, envconfig.Process("", cfg)
}

// String is implemented to prevent sensitive fields being logged.
// The config is returned as JSON with sensitive fields omitted.
func (config Configuration) String() string {
	b, _ := json.Marshal(config)
	return string(b)
}
