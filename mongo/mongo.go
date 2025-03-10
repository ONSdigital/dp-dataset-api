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

	Connection                   *mongodriver.MongoConnection
	healthClient                 *mongohealth.CheckMongoClient
	lockClientInstanceCollection *mongolock.Lock
	lockClientVersionsCollection *mongolock.Lock
}

// Init returns an initialised Mongo object encapsulating a connection to the mongo server/cluster with the given configuration,
// a health client to check the health of the mongo server/cluster, and a lock client
func (m *Mongo) Init(ctx context.Context) (err error) {
	m.Connection, err = mongodriver.Open(&m.MongoDriverConfig)
	if err != nil {
		return err
	}

	databaseCollectionBuilder := map[mongohealth.Database][]mongohealth.Collection{
		mongohealth.Database(m.Database): {
			mongohealth.Collection(m.ActualCollectionName(config.DatasetsCollection)),
			mongohealth.Collection(m.ActualCollectionName(config.EditionsCollection)),
			mongohealth.Collection(m.ActualCollectionName(config.InstanceCollection)),
			mongohealth.Collection(m.ActualCollectionName(config.DimensionOptionsCollection)),
			mongohealth.Collection(m.ActualCollectionName(config.InstanceLockCollection)),
			mongohealth.Collection(m.ActualCollectionName(config.VersionsCollection)),
		},
	}
	m.healthClient = mongohealth.NewClientWithCollections(m.Connection, databaseCollectionBuilder)
	m.lockClientInstanceCollection = mongolock.New(ctx, m.Connection, m.ActualCollectionName(config.InstanceCollection))
	m.lockClientVersionsCollection = mongolock.New(ctx, m.Connection, m.ActualCollectionName(config.VersionsCollection))

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
