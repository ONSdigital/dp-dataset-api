package config

import "github.com/ian-kent/gofigure"

// Configuration structure which hold information for configuring the import API
type Configuration struct {
	BindAddr      string `env:"BIND_ADDR" flag:"bind-addr" flagDesc:"The port to bind to"`
	DatasetAPIURL string `env:"DATASET_API_URL" flag:"dataset-api-url" flagDesc:"The host and port this API is run on"`
	CodeListAPIURL string `env:"CODE_LIST_API_URL" flag:"code-list-api-url" flagDesc:"The host and port for the code list API"`
	SecretKey     string `env:"SECRET_KEY" flag:"secret-key" flagDesc:"A secret key used authentication"`
	MongoConfig   MongoConfig
}

// MongoConfig contains the config required to connect to MongoDB.
type MongoConfig struct {
	BindAddr   string `env:"MONGODB_BIND_ADDR" flag:"mongodb-bind-addr" flagDesc:"MongoDB bind address"`
	Collection string `env:"MONGODB_DATABASE" flag:"mongodb-database" flagDesc:"MongoDB database"`
	Database   string `env:"MONGODB_COLLECTION" flag:"mongodb-collection" flagDesc:"MongoDB collection"`
}

var cfg *Configuration

// Get the application and returns the configuration structure
func Get() (*Configuration, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Configuration{
		BindAddr:      ":22000",
		DatasetAPIURL: "http://localhost:22000",
		CodeListAPIURL: "http://localhost:22400",
		SecretKey:     "FD0108EA-825D-411C-9B1D-41EF7727F465",
		MongoConfig: MongoConfig{
			BindAddr:   "localhost:27017",
			Collection: "datasets",
			Database:   "datasets",
		},
	}

	return cfg, gofigure.Gofigure(cfg)

}
