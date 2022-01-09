package mongo

import (
	"context"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"

	mongolock "github.com/ONSdigital/dp-mongodb/v3/dplock"
	mongohealth "github.com/ONSdigital/dp-mongodb/v3/health"
	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
)

type Mongo struct {
	config.MongoConfig

	Connection   *mongodriver.MongoConnection
	healthClient *mongohealth.CheckMongoClient
	lockClient   *mongolock.Lock
}

const (
	editionsCollection     = "editions"
	instanceCollection     = "instances"
	instanceLockCollection = "instances_locks"
	dimensionOptions       = "dimension.options"
)

// Init returns an initialised Mongo object encapsulating a connection to the mongo server/cluster with the given configuration,
// a health client to check the health of the mongo server/cluster, and a lock client
func (m *Mongo) Init(ctx context.Context) (err error) {

	m.Connection, err = mongodriver.Open(&m.MongoConnectionConfig)
	if err != nil {
		return err
	}

	databaseCollectionBuilder := map[mongohealth.Database][]mongohealth.Collection{
		(mongohealth.Database)(m.Database): {editionsCollection, instanceCollection, instanceLockCollection, dimensionOptions}}
	m.healthClient = mongohealth.NewClientWithCollections(m.Connection, databaseCollectionBuilder)
	m.lockClient = mongolock.New(ctx, m.Connection, instanceCollection)

	return nil
}

// Close represents mongo session closing within the context deadline
func (m *Mongo) Close(ctx context.Context) error {
	return m.Connection.Close(ctx)
}

// Checker is called by the healthcheck library to check the health state of this mongoDB instance
func (m *Mongo) Checker(ctx context.Context, state *healthcheck.CheckState) error {
	return m.healthClient.Checker(ctx, state)
}
