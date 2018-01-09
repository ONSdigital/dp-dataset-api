package config

import (
	"time"

	"github.com/ONSdigital/go-ns/log"
	"github.com/kelseyhightower/envconfig"
)

// Configuration structure which hold information for configuring the import API
type Configuration struct {
	BindAddr                string        `envconfig:"BIND_ADDR"`
	KafkaAddr               []string      `envconfig:"KAFKA_ADDR"`
	GenerateDownloadsTopic  string        `envconfig:"GENERATE_DOWNLOADS_TOPIC"`
	CodeListAPIURL          string        `envconfig:"CODE_LIST_API_URL"`
	DatasetAPIURL           string        `envconfig:"DATASET_API_URL"`
	WebsiteURL              string        `envconfig:"WEBSITE_URL"`
	SecretKey               string        `envconfig:"SECRET_KEY"`
	GracefulShutdownTimeout time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	HealthCheckTimeout      time.Duration `envconfig:"HEALTHCHECK_TIMEOUT"`
	MongoConfig             MongoConfig
}

// MongoConfig contains the config required to connect to MongoDB.
type MongoConfig struct {
	BindAddr   string `envconfig:"MONGODB_BIND_ADDR"`
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
		BindAddr:                ":22000",
		KafkaAddr:               []string{"localhost:9092"},
		GenerateDownloadsTopic:  "filter-job-submitted",
		CodeListAPIURL:          "http://localhost:22400",
		DatasetAPIURL:           "http://localhost:22000",
		WebsiteURL:              "http://localhost:20000",
		SecretKey:               "FD0108EA-825D-411C-9B1D-41EF7727F465",
		GracefulShutdownTimeout: 5 * time.Second,
		HealthCheckTimeout:      2 * time.Second,
		MongoConfig: MongoConfig{
			BindAddr:   "localhost:27017",
			Collection: "datasets",
			Database:   "datasets",
		},
	}

	sanitized := *cfg
	sanitized.SecretKey = ""
	log.Info("config on startup", log.Data{"config": sanitized})

	return cfg, envconfig.Process("", cfg)
}
