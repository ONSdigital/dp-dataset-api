package mongo

import (
	"context"
	"time"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	mim "github.com/ONSdigital/dp-mongodb-in-memory"
	mongoDriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
)

var (
	staticDatasetID      = "staticDatasetID123"
	staticDatasetID2     = "staticDatasetID456"
	nonExistentDatasetID = "nonExistentDatasetID"
)

// getTestMongoDB initializes a MongoDB connection for use in tests
func getTestMongoDB(ctx context.Context) (*Mongo, *mim.Server, error) {
	mongoVersion := "4.4.8"

	cfg, err := config.Get()
	if err != nil {
		return nil, nil, err
	}

	mongoServer, err := mim.Start(ctx, mongoVersion)
	if err != nil {
		return nil, nil, err
	}
	mongoConfig := getTestMongoDriverConfig(mongoServer, cfg.Database, cfg.Collections)
	conn, err := mongoDriver.Open(mongoConfig)
	if err != nil {
		return nil, nil, err
	}

	return &Mongo{
		MongoConfig: cfg.MongoConfig,
		Connection:  conn,
	}, mongoServer, nil
}

// Custom config to work with mongo in memory
func getTestMongoDriverConfig(mongoServer *mim.Server, database string, collections map[string]string) *mongoDriver.MongoDriverConfig {
	return &mongoDriver.MongoDriverConfig{
		ConnectTimeout:  5 * time.Second,
		QueryTimeout:    5 * time.Second,
		ClusterEndpoint: mongoServer.URI(),
		Database:        database,
		Collections:     collections,
	}
}

func setupVersionsTestData(ctx context.Context, mongoStore *Mongo) ([]*models.Version, error) {
	if err := mongoStore.Connection.DropDatabase(ctx); err != nil {
		return nil, err
	}

	now := time.Now()

	versions := []*models.Version{
		{
			ID:           "version1",
			Edition:      "edition1",
			EditionTitle: "First Edition",
			LastUpdated:  now,
			Version:      1,
			State:        "published",
			Type:         "static",
			ETag:         "version1ETag",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{ID: staticDatasetID},
				Edition: &models.LinkObject{ID: "edition1"},
				Version: &models.LinkObject{ID: "1"},
			},
		},
		{
			ID:           "version2",
			Edition:      "edition2",
			EditionTitle: "Second Edition",
			LastUpdated:  now.Add(time.Hour),
			Version:      2,
			State:        "edition-confirmed",
			Type:         "static",
			ETag:         "version2ETag",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{ID: staticDatasetID},
				Edition: &models.LinkObject{ID: "edition2"},
				Version: &models.LinkObject{ID: "2"},
			},
		},
		{
			ID:           "newedition",
			Edition:      "newedition",
			EditionTitle: "New Edition",
			LastUpdated:  now,
			Version:      1,
			State:        "published",
			Type:         "static",
			ETag:         "newETag",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{ID: staticDatasetID2},
				Edition: &models.LinkObject{ID: "newedition"},
				Version: &models.LinkObject{ID: "1"},
			},
		},
	}

	for _, v := range versions {
		if _, err := mongoStore.Connection.Collection(mongoStore.ActualCollectionName(config.VersionsCollection)).InsertOne(ctx, v); err != nil {
			return nil, err
		}
	}

	return versions, nil
}
