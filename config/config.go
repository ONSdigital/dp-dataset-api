package config

import (
	"encoding/json"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// MongoConfig contains the config required to connect to MongoDB.
type MongoConfig struct {
	URI                string        `envconfig:"MONGODB_BIND_ADDR"   json:"-"`
	Collection         string        `envconfig:"MONGODB_COLLECTION"`
	Database           string        `envconfig:"MONGODB_DATABASE"`
	Username           string        `envconfig:"MONGODB_USERNAME"    json:"-"`
	Password           string        `envconfig:"MONGODB_PASSWORD"    json:"-"`
	IsSSL              bool          `envconfig:"MONGODB_IS_SSL"`
	EnableReadConcern  bool          `envconfig:"MONGODB_ENABLE_READ_CONCERN"`
	EnableWriteConcern bool          `envconfig:"MONGODB_ENABLE_WRITE_CONCERN"`
	QueryTimeout       time.Duration `envconfig:"MONGODB_QUERY_TIMEOUT"`
	ConnectionTimeout  time.Duration `envconfig:"MONGODB_CONNECT_TIMEOUT"`

	CodeListAPIURL string `envconfig:"CODE_LIST_API_URL"`
	DatasetAPIURL  string `envconfig:"DATASET_API_URL"`
}

// Configuration structure which hold information for configuring the import API
type Configuration struct {
	BindAddr                   string        `envconfig:"BIND_ADDR"`
	KafkaAddr                  []string      `envconfig:"KAFKA_ADDR"                       json:"-"`
	KafkaSecProtocol           string        `envconfig:"KAFKA_SEC_PROTO"`
	KafkaSecCACerts            string        `envconfig:"KAFKA_SEC_CA_CERTS"`
	KafkaSecClientCert         string        `envconfig:"KAFKA_SEC_CLIENT_CERT"`
	KafkaSecClientKey          string        `envconfig:"KAFKA_SEC_CLIENT_KEY"             json:"-"`
	KafkaSecSkipVerify         bool          `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
	GenerateDownloadsTopic     string        `envconfig:"GENERATE_DOWNLOADS_TOPIC"`
	WebsiteURL                 string        `envconfig:"WEBSITE_URL"`
	ZebedeeURL                 string        `envconfig:"ZEBEDEE_URL"`
	DownloadServiceSecretKey   string        `envconfig:"DOWNLOAD_SERVICE_SECRET_KEY"      json:"-"`
	ServiceAuthToken           string        `envconfig:"SERVICE_AUTH_TOKEN"               json:"-"`
	GracefulShutdownTimeout    time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	HealthCheckInterval        time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	HealthCheckCriticalTimeout time.Duration `envconfig:"HEALTHCHECK_CRITICAL_TIMEOUT"`
	EnablePrivateEndpoints     bool          `envconfig:"ENABLE_PRIVATE_ENDPOINTS"`
	EnableDetachDataset        bool          `envconfig:"ENABLE_DETACH_DATASET"`
	EnablePermissionsAuth      bool          `envconfig:"ENABLE_PERMISSIONS_AUTH"`
	EnableObservationEndpoint  bool          `envconfig:"ENABLE_OBSERVATION_ENDPOINT"`
	DisableGraphDBDependency   bool          `envconfig:"DISABLE_GRAPH_DB_DEPENDENCY"`
	KafkaVersion               string        `envconfig:"KAFKA_VERSION"`
	DefaultMaxLimit            int           `envconfig:"DEFAULT_MAXIMUM_LIMIT"`
	DefaultLimit               int           `envconfig:"DEFAULT_LIMIT"`
	DefaultOffset              int           `envconfig:"DEFAULT_OFFSET"`
	MaxRequestOptions          int           `envconfig:"MAX_REQUEST_OPTIONS"`
	MongoConfig
}

var cfg *Configuration

// Get the application and returns the configuration structure
func Get() (*Configuration, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Configuration{
		BindAddr:                   ":22000",
		KafkaAddr:                  []string{"localhost:9092"},
		GenerateDownloadsTopic:     "filter-job-submitted",
		WebsiteURL:                 "http://localhost:20000",
		ZebedeeURL:                 "http://localhost:8082",
		ServiceAuthToken:           "FD0108EA-825D-411C-9B1D-41EF7727F465",
		DownloadServiceSecretKey:   "QB0108EZ-825D-412C-9B1D-41EF7747F462",
		GracefulShutdownTimeout:    5 * time.Second,
		HealthCheckInterval:        30 * time.Second,
		HealthCheckCriticalTimeout: 90 * time.Second,
		EnablePrivateEndpoints:     false,
		EnableDetachDataset:        false,
		EnablePermissionsAuth:      false,
		EnableObservationEndpoint:  true,
		DisableGraphDBDependency:   false,
		KafkaVersion:               "1.0.2",
		DefaultMaxLimit:            1000,
		DefaultLimit:               20,
		DefaultOffset:              0,
		MaxRequestOptions:          100, // Maximum number of options acceptable in an incoming Patch request. Compromise between one option per call (inefficient) and an order of 100k options per call, for census data (memory and computationally expensive)
		MongoConfig: MongoConfig{
			URI:                "localhost:27017",
			Database:           "datasets",
			Collection:         "datasets",
			QueryTimeout:       15 * time.Second,
			ConnectionTimeout:  5 * time.Second,
			EnableWriteConcern: true,

			CodeListAPIURL: "http://localhost:22400",
			DatasetAPIURL:  "http://localhost:22000",
		},
	}

	return cfg, envconfig.Process("", cfg)
}

// String is implemented to prevent sensitive fields being logged.
// The config is returned as JSON with sensitive fields omitted.
func (config Configuration) String() string {
	b, _ := json.Marshal(config)
	return string(b)
}
