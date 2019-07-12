package config

import (
	"errors"

	"github.com/ONSdigital/dp-graph/graph/driver"
	"github.com/ONSdigital/dp-graph/mock"
	"github.com/ONSdigital/dp-graph/neo4j"
	"github.com/ONSdigital/dp-graph/neptune"
	"github.com/kelseyhightower/envconfig"
)

// Configuration allows environment variables to be read and sent to the
// relevant driver for further setup
type Configuration struct {
	DriverChoice    string `envconfig:"GRAPH_DRIVER_TYPE"`
	DatabaseAddress string `envconfig:"GRAPH_ADDR"`
	PoolSize        int    `envconfig:"GRAPH_POOL_SIZE"`
	MaxRetries      int    `envconfig:"MAX_RETRIES"`
	QueryTimeout    int    `envconfig:"GRAPH_QUERY_TIMEOUT"`

	Driver driver.Driver
}

var cfg *Configuration

// Get reads config and returns the configured instantiated driver
func Get(errs chan error) (*Configuration, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Configuration{
		DriverChoice: "",
	}

	err := envconfig.Process("", cfg)

	var d driver.Driver

	switch cfg.DriverChoice {
	case "neo4j":
		d, err = neo4j.New(cfg.DatabaseAddress, cfg.PoolSize, cfg.QueryTimeout, cfg.MaxRetries)
		if err != nil {
			return nil, err
		}
	case "neptune":
		d, err = neptune.New(cfg.DatabaseAddress, cfg.PoolSize, cfg.QueryTimeout, cfg.MaxRetries, errs)
		if err != nil {
			return nil, err
		}
	case "mock":
		d = &mock.Mock{}
	default:
		return nil, errors.New("driver type config not provided")
	}

	cfg.Driver = d

	return cfg, nil
}
