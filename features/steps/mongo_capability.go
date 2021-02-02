package steps_test

import (
	"log"
	"time"

	"github.com/benweissmann/memongo"
)

// MongoCapability is a struct containing an in-memory mongo database
type MongoCapability struct {
	Server *memongo.Server
}

// MongoOptions contains a set of options required to create a new MongoCapability
type MongoOptions struct {
	Port         int
	MongoVersion string
	Logger       *log.Logger
}

// NewMongoCapability creates a new in-memory mongo database using the supplied options
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

// Reset is currently not implemented
func (m *MongoCapability) Reset() error {
	return nil
}

// Close stops the in-memory mongo database
func (m *MongoCapability) Close() error {
	m.Server.Stop()
	return nil
}
