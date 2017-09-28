package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Configuration structure which hold information for configuring the import API
type Configuration struct {
	BindAddr        string        `envconfig:"BIND_ADDR"`
	CodeListAPIURL  string        `env:"CODE_LIST_API_URL"`
	DatasetAPIURL   string        `envconfig:"DATASET_API_URL"`
	SecretKey       string        `envconfig:"SECRET_KEY"`
	ShutdownTimeout time.Duration `envconfig:"SHUTDOWN_TIMEOUT"`
	MongoConfig     MongoConfig
}

// MongoConfig contains the config required to connect to MongoDB.
type MongoConfig struct {
	BindAddr   string `envconfig:"MONGODB_BIND_ADDR"`
	Collection string `envconfig:"MONGODB_DATABASE"`
	Database   string `envconfig:"MONGODB_COLLECTION"`
}

var cfg *Configuration

// Get the application and returns the configuration structure
func Get() (*Configuration, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Configuration{
		BindAddr:        ":22000",
		CodeListAPIURL:  "http://localhost:22400",
		DatasetAPIURL:   "http://localhost:22000",
		SecretKey:       "FD0108EA-825D-411C-9B1D-41EF7727F465",
		ShutdownTimeout: 5 * time.Second,
		MongoConfig: MongoConfig{
			BindAddr:   "localhost:27017",
			Collection: "datasets",
			Database:   "datasets",
		},
	}

	return cfg, envconfig.Process("", cfg)
}
