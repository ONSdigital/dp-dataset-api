package config

import (
	"encoding/json"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Configuration structure which hold information for configuring the import API
type Configuration struct {
	BindAddr                   string        `envconfig:"BIND_ADDR"`
	KafkaAddr                  []string      `envconfig:"KAFKA_ADDR"                       json:"-"`
	GenerateDownloadsTopic     string        `envconfig:"GENERATE_DOWNLOADS_TOPIC"`
	CodeListAPIURL             string        `envconfig:"CODE_LIST_API_URL"`
	DatasetAPIURL              string        `envconfig:"DATASET_API_URL"`
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
	MongoConfig                MongoConfig
}

// MongoConfig contains the config required to connect to MongoDB.
type MongoConfig struct {
	BindAddr   string `envconfig:"MONGODB_BIND_ADDR"   json:"-"`
	Collection string `envconfig:"MONGODB_COLLECTION"`
	Database   string `envconfig:"MONGODB_DATABASE"`
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
		CodeListAPIURL:             "http://localhost:22400",
		DatasetAPIURL:              "http://localhost:22000",
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
		MongoConfig: MongoConfig{
			BindAddr:   "localhost:27017",
			Collection: "datasets",
			Database:   "datasets",
		},
	}

	return cfg, envconfig.Process("", cfg)
}

// String is implemented to prevent sensitive fields being logged.
// The config is returned as JSON with sensitive fields omitted.
func (config Configuration) String() string {
	json, _ := json.Marshal(config)
	return string(json)
}
