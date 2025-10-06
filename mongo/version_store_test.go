package mongo

import (
	"context"
	"testing"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	staticDatasetID      = "staticDatasetID123"
	nonExistentDatasetID = "nonExistentDatasetID"
)

// setupStaticVersionsTestData populates the in-memory database with static version documents
func setupStaticVersionsTestData(ctx context.Context, mongoStore *Mongo) ([]*models.Version, error) {
	// Drop the database to ensure a clean slate
	if err := mongoStore.Connection.DropDatabase(ctx); err != nil {
		return nil, err
	}

	now := time.Now()
	versions := []*models.Version{
		{
			ID:           "version1",
			Edition:      "editionA",
			EditionTitle: "First Edition A",
			LastUpdated:  now,
			Version:      1,
			State:        "edition-confirmed",
			Type:         "static",
			ETag:         "version1ETag",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{ID: staticDatasetID},
			},
		},
		{
			ID:           "version2",
			Edition:      "editionB",
			EditionTitle: "First Edition B",
			LastUpdated:  now,
			Version:      1,
			State:        "published",
			Type:         "static",
			ETag:         "version2ETag",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{ID: staticDatasetID},
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

func TestUpdateVersionStatic(t *testing.T) {
	Convey("Given a current version, version update and etag", t, func() {
		ctx := context.Background()

		mongoDB, _, err := getTestMongoDB(ctx)
		So(err, ShouldBeNil)

		_, err = setupVersionsTestData(ctx, mongoDB)
		So(err, ShouldBeNil)

		currentVersion := &models.Version{ID: "version1", Edition: "edition1", Version: 1, ETag: "version1ETag"}
		versionUpdate := &models.Version{EditionTitle: "First Edition Updated"}
		oldETag := "version1ETag"

		Convey("When UpdateVersionStatic is called and the version exists", func() {
			newEtag, err := mongoDB.UpdateVersionStatic(ctx, currentVersion, versionUpdate, oldETag)

			Convey("Then the version is updated successfully", func() {
				So(err, ShouldBeNil)
				So(newEtag, ShouldNotEqual, oldETag)
			})

			Convey("And the version is updated in the database", func() {
				var updatedVersion models.Version
				err = mongoDB.Connection.Collection(mongoDB.ActualCollectionName(config.VersionsCollection)).FindOne(ctx, map[string]string{"id": currentVersion.ID}, &updatedVersion)
				So(err, ShouldBeNil)
				So(updatedVersion.EditionTitle, ShouldEqual, "First Edition Updated")
				So(updatedVersion.ETag, ShouldEqual, newEtag)
			})
		})

		Convey("When UpdateVersionStatic is called and the version does not exist", func() {
			_, err := mongoDB.UpdateVersionStatic(ctx, &models.Version{}, versionUpdate, oldETag)

			Convey("Then a VersionNotFound error is returned", func() {
				So(err, ShouldEqual, errs.ErrVersionNotFound)
			})
		})
	})
}

func TestGetStaticDatasetVersions(t *testing.T) {
	Convey("Given an in-memory MongoDB is running and populated with static versions", t, func() {
		ctx := context.Background()
		mongoStore, server, err := getTestMongoDB(ctx)
		So(err, ShouldBeNil)

		// Defer cleanup
		defer func() {
			server.Stop(ctx)
		}()

		versions, err := setupStaticVersionsTestData(ctx, mongoStore)
		So(err, ShouldBeNil)
		So(versions, ShouldNotBeEmpty)

		Convey("When GetStaticDatasetVersions is called with a matching datasetID", func() {
			retrievedVersions, count, err := mongoStore.GetStaticDatasetVersions(ctx, staticDatasetID, 0, 0)

			So(err, ShouldBeNil)
			So(count, ShouldEqual, 2)
			So(retrievedVersions, ShouldHaveLength, 2)

			// Assert that the documents returned are the ones we inserted
			So(retrievedVersions[0].ID, ShouldEqual, "version1")
			So(retrievedVersions[1].ID, ShouldEqual, "version2")
		})

		Convey("When GetStaticDatasetVersions is called with a non-matching datasetID", func() {
			retrievedVersions, count, err := mongoStore.GetStaticDatasetVersions(ctx, nonExistentDatasetID, 0, 0)

			So(err, ShouldEqual, errs.ErrVersionsNotFound)
			So(count, ShouldEqual, 0)
			So(retrievedVersions, ShouldBeNil)
		})
	})
}

func TestDeleteStaticDatasetVersion(t *testing.T) {
	Convey("Given an in-memory MongoDB is running and populated with static versions", t, func() {
		ctx := context.Background()
		mongoStore, server, err := getTestMongoDB(ctx)
		So(err, ShouldBeNil)

		// Defer cleanup
		defer func() {
			server.Stop(ctx)
		}()

		versions, err := setupStaticVersionsTestData(ctx, mongoStore)
		So(err, ShouldBeNil)
		So(versions, ShouldHaveLength, 2)

		datasetIDToDelete := versions[0].Links.Dataset.ID

		Convey("When DeleteStaticDatasetVersion is called with a matching datasetID", func() {
			err := mongoStore.DeleteStaticDatasetVersion(ctx, datasetIDToDelete)
			So(err, ShouldBeNil)

			// Verify all versions linked to that datasetID are deleted
			selector := bson.M{"links.dataset.id": datasetIDToDelete}
			totalCount, err := mongoStore.Connection.Collection(mongoStore.ActualCollectionName(config.VersionsCollection)).Count(ctx, selector)
			So(err, ShouldBeNil)
			So(totalCount, ShouldEqual, 0)
		})

		Convey("When DeleteStaticDatasetVersion is called for a dataset with no versions", func() {
			_, _, err := mongoStore.GetStaticDatasetVersions(ctx, staticDatasetID, 0, 0)
			So(err, ShouldEqual, errs.ErrVersionsNotFound)

			err = mongoStore.DeleteStaticDatasetVersion(ctx, nonExistentDatasetID)
			So(err, ShouldBeNil)
		})
	})
}
