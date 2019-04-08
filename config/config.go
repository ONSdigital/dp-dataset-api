package config

import (
	"encoding/json"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Configuration structure which hold information for configuring the import API
type Configuration struct {
	BindAddr                 string        `envconfig:"BIND_ADDR"`
	FeatureDetachDataset     bool          `envconfig:"FEATURE_DETACH_DATASET"`
	KafkaAddr                []string      `envconfig:"KAFKA_ADDR"                       json:"-"`
	AuditEventsTopic         string        `envconfig:"AUDIT_EVENTS_TOPIC"`
	GenerateDownloadsTopic   string        `envconfig:"GENERATE_DOWNLOADS_TOPIC"`
	CodeListAPIURL           string        `envconfig:"CODE_LIST_API_URL"`
	DatasetAPIURL            string        `envconfig:"DATASET_API_URL"`
	WebsiteURL               string        `envconfig:"WEBSITE_URL"`
	ZebedeeURL               string        `envconfig:"ZEBEDEE_URL"`
	DownloadServiceSecretKey string        `envconfig:"DOWNLOAD_SERVICE_SECRET_KEY"      json:"-"`
	ServiceAuthToken         string        `envconfig:"SERVICE_AUTH_TOKEN"               json:"-"`
	GracefulShutdownTimeout  time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	HealthCheckInterval      time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	HealthCheckTimeout       time.Duration `envconfig:"HEALTHCHECK_TIMEOUT"`
	EnablePrivateEnpoints    bool          `envconfig:"ENABLE_PRIVATE_ENDPOINTS"`
	Neo4jBindAddress         string        `envconfig:"NEO4J_BIND_ADDRESS" json:"-"`
	Neo4jPoolSize            int           `envconfig:"NEO4J_POOL_SIZE"`
	MongoConfig              MongoConfig
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
		BindAddr:                 ":22000",
		FeatureDetachDataset:     false,
		KafkaAddr:                []string{"localhost:9092"},
		AuditEventsTopic:         "audit-events",
		GenerateDownloadsTopic:   "filter-job-submitted",
		CodeListAPIURL:           "http://localhost:22400",
		DatasetAPIURL:            "http://localhost:22000",
		WebsiteURL:               "http://localhost:20000",
		ZebedeeURL:               "http://localhost:8082",
		ServiceAuthToken:         "FD0108EA-825D-411C-9B1D-41EF7727F465",
		DownloadServiceSecretKey: "QB0108EZ-825D-412C-9B1D-41EF7747F462",
		GracefulShutdownTimeout:  5 * time.Second,
		HealthCheckInterval:      30 * time.Second,
		HealthCheckTimeout:       2 * time.Second,
		EnablePrivateEnpoints:    false,
		Neo4jBindAddress:         "bolt://localhost:7687",
		Neo4jPoolSize:            5,
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
