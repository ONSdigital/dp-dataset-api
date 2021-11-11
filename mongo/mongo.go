package mongo

import (
	"context"
	"errors"
	"fmt"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"

	mongolock "github.com/ONSdigital/dp-mongodb/v3/dplock"
	mongohealth "github.com/ONSdigital/dp-mongodb/v3/health"
	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"

	dpmongo "github.com/ONSdigital/dp-mongodb"
	dpMongoHealth "github.com/ONSdigital/dp-mongodb/health"

	"github.com/globalsign/mgo"
)

// Mongo represents a simplistic MongoDB configuration.
type Mongo struct {
	config.MongoConfig

	Connection   *mongodriver.MongoConnection
	healthClient *mongohealth.CheckMongoClient
	lockClient   *mongolock.Lock

	Session *mgo.Session
	hc      *dpMongoHealth.CheckMongoClient
}

func (m *Mongo) getConnectionConfig() *mongodriver.MongoConnectionConfig {
	return &mongodriver.MongoConnectionConfig{
		ClusterEndpoint:               m.URI,
		Username:                      m.Username,
		Password:                      m.Password,
		Database:                      m.Database,
		Collection:                    m.Collection,
		IsWriteConcernMajorityEnabled: m.EnableWriteConcern,
		IsStrongReadConcernEnabled:    m.EnableReadConcern,

		IsSSL:                   m.IsSSL,
		ConnectTimeoutInSeconds: m.ConnectionTimeout,
		QueryTimeoutInSeconds:   m.QueryTimeout,
	}
}

const (
	editionsCollection     = "editions"
	instanceCollection     = "instances"
	instanceLockCollection = "instances_locks"
	dimensionOptions       = "dimension.options"
)

// Init creates a new mgo.Session with a strong consistency and a write mode of "majority"; and initialises the mongo health client.
func (m *Mongo) Init(ctx context.Context) (err error) {

	m.Connection, err = mongodriver.Open(m.getConnectionConfig())
	if err != nil {
		return err
	}

	databaseCollectionBuilder := make(map[mongohealth.Database][]mongohealth.Collection)
	databaseCollectionBuilder[(mongohealth.Database)(m.Database)] = []mongohealth.Collection{(mongohealth.Collection)(m.Collection), editionsCollection, instanceCollection, instanceLockCollection, dimensionOptions}

	// Create client and healthclient from session
	client := mongohealth.NewClientWithCollections(m.Connection, databaseCollectionBuilder)
	m.healthClient = &mongohealth.CheckMongoClient{
		Client:      *client,
		Healthcheck: client.Healthcheck,
	}

	// Create MongoDB lock client, which also starts the purger loop
	m.lockClient = mongolock.New(ctx, m.Connection, instanceCollection)
	if err != nil {
		return err
	}

	if m.Session != nil {
		return errors.New("session already exists")
	}

	// Create session
	if m.Session, err = mgo.Dial(m.URI); err != nil {
		return err
	}
	m.Session.EnsureSafe(&mgo.Safe{WMode: "majority"})
	m.Session.SetMode(mgo.Strong, true)

	dcb := make(map[dpMongoHealth.Database][]dpMongoHealth.Collection)
	dcb[(dpMongoHealth.Database)(m.Database)] = []dpMongoHealth.Collection{(dpMongoHealth.Collection)(m.Collection), editionsCollection, instanceCollection, instanceLockCollection, dimensionOptions}

	// Create client and healthclient from session
	cl := dpMongoHealth.NewClientWithCollections(m.Session, dcb)
	m.hc = &dpMongoHealth.CheckMongoClient{
		Client:      *cl,
		Healthcheck: cl.Healthcheck,
	}

	return nil
}

// Close represents mongo session closing within the context deadline
func (m *Mongo) Close(ctx context.Context) error {
	//return m.Connection.Close(ctx)

	return jointError(dpmongo.Close(ctx, m.Session), m.Connection.Close(ctx))
}

// Checker is called by the healthcheck library to check the health state of this mongoDB instance
func (m *Mongo) Checker(ctx context.Context, state *healthcheck.CheckState) error {
	//return m.healthClient.Checker(ctx, state)

	return jointError(m.hc.Checker(ctx, state), m.healthClient.Checker(ctx, state))
}

func jointError(old, new error) error {
	switch {
	case old != nil && new != nil:
		return errors.New(fmt.Sprintf("olddriver: %s; newdriver: %s", old.Error(), new.Error()))
	case old != nil:
		return old
	case new != nil:
		return new
	default:
		return nil
	}
}
