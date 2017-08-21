package config

import "github.com/ian-kent/gofigure"

// Configuration structure which hold information for configuring the import API
type Configuration struct {
	BindAddr            string `env:"BIND_ADDR" flag:"bind-addr" flagDesc:"The port to bind to"`
	PostgresDatasetsURL string `env:"POSTGRES_DATASETS_URL" flag:"postgres-dataset-url" flagDesc:"The URL address to connect to a postgres instance of the dataset database'"`
	SecretKey           string `env:"SECRET_KEY" flag:"secret-key" flagDesc:"A secret key used authentication"`
}

var cfg *Configuration

// Get the application and returns the configuration structure
func Get() (*Configuration, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Configuration{
		BindAddr:            ":22000",
		PostgresDatasetsURL: "user=dp dbname=Datasets sslmode=disable",
		SecretKey:           "FD0108EA-825D-411C-9B1D-41EF7727F465",
	}

	return cfg, gofigure.Gofigure(cfg)

}
