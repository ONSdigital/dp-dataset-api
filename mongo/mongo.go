package mongo

import (
	"context"
	"errors"
	"time"

	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	dpmongo "github.com/ONSdigital/dp-mongodb"
	dpMongoLock "github.com/ONSdigital/dp-mongodb/dplock"
	dpMongoHealth "github.com/ONSdigital/dp-mongodb/health"
	"github.com/globalsign/mgo"
)

// Mongo represents a simplistic MongoDB configuration.
type Mongo struct {
	CodeListURL    string
	Collection     string
	Database       string
	DatasetURL     string
	Session        *mgo.Session
	URI            string
	lastPingTime   time.Time
	lastPingResult error
	healthClient   *dpMongoHealth.CheckMongoClient
	lockClient     *dpMongoLock.Lock
}

const (
	editionsCollection     = "editions"
	instanceCollection     = "instances"
	instanceLockCollection = "instances_locks"
	dimensionOptions       = "dimension.options"
)

// Init creates a new mgo.Session with a strong consistency and a write mode of "majortiy"; and initialises the mongo health client.
func (m *Mongo) Init(ctx context.Context) (err error) {
	if m.Session != nil {
		return errors.New("session already exists")
	}

	// Create session
	if m.Session, err = mgo.Dial(m.URI); err != nil {
		return err
	}
	m.Session.EnsureSafe(&mgo.Safe{WMode: "majority"})
	m.Session.SetMode(mgo.Strong, true)

	databaseCollectionBuilder := make(map[dpMongoHealth.Database][]dpMongoHealth.Collection)
	databaseCollectionBuilder[(dpMongoHealth.Database)(m.Database)] = []dpMongoHealth.Collection{(dpMongoHealth.Collection)(m.Collection), (dpMongoHealth.Collection)(editionsCollection), (dpMongoHealth.Collection)(instanceCollection), (dpMongoHealth.Collection)(instanceLockCollection), (dpMongoHealth.Collection)(dimensionOptions)}

	// Create client and healthclient from session
	client := dpMongoHealth.NewClientWithCollections(m.Session, databaseCollectionBuilder)
	m.healthClient = &dpMongoHealth.CheckMongoClient{
		Client:      *client,
		Healthcheck: client.Healthcheck,
	}

	// Create MongoDB lock client, which also starts the purger loop
	m.lockClient, err = dpMongoLock.New(ctx, m.Session, m.Database, instanceCollection, nil)
	if err != nil {
		return err
	}
	return nil
}

// Close represents mongo session closing within the context deadline
func (m *Mongo) Close(ctx context.Context) error {
	if m.Session == nil {
		return errors.New("cannot close a mongoDB connection without a valid session")
	}
	return dpmongo.Close(ctx, m.Session)
}

// Checker is called by the healthcheck library to check the health state of this mongoDB instance
func (m *Mongo) Checker(ctx context.Context, state *healthcheck.CheckState) error {
	return m.healthClient.Checker(ctx, state)
}
