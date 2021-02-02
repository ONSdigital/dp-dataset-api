package steps_test

import (
	"log"
	"time"

	"github.com/benweissmann/memongo"
)

type MongoCapability struct {
	Server *memongo.Server
}
type MongoOptions struct {
	Port         int
	MongoVersion string
	Logger       *log.Logger
}

func NewMongoCapability(options MongoOptions) *MongoCapability {

	opts := memongo.Options{
		Port:           options.Port,
		MongoVersion:   options.MongoVersion,
		StartupTimeout: time.Second * 10,
		Logger:         options.Logger,
	}

	mongoServer, err := memongo.StartWithOptions(&opts)
	if err != nil {
		panic(err)
	}

	return &MongoCapability{
		Server: mongoServer,
	}
}

func (m *MongoCapability) Reset() error {
	return nil
}

func (m *MongoCapability) Close() error {
	m.Server.Stop()
	return nil
}
