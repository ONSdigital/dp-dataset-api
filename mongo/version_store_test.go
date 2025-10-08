package mongo

import (
	"context"
	"testing"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
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
			State:        "published",
			Type:         "static",
			ETag:         "version1ETag",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{ID: staticDatasetID},
			},
		},
		{
			ID:           "version2",
			Edition:      "editionB",
			EditionTitle: "Second Edition B",
			LastUpdated:  now.Add(time.Hour),
			Version:      2,
			State:        "edition-confirmed",
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

func TestGetAllStaticVersions(t *testing.T) {
	Convey("Given an in-memory MongoDB is running and populated with static versions", t, func() {
		ctx := context.Background()
		mongoStore, server, err := getTestMongoDB(ctx)
		So(err, ShouldBeNil)

		defer func() {
			server.Stop(ctx)
		}()

		versions, err := setupStaticVersionsTestData(ctx, mongoStore)
		So(err, ShouldBeNil)
		So(versions, ShouldNotBeEmpty)

		Convey("When GetAllStaticVersions is called with offset=0 and limit=0", func() {
			retrievedVersions, count, err := mongoStore.GetAllStaticVersions(ctx, staticDatasetID, "", 0, 0)

			So(err, ShouldBeNil)
			So(count, ShouldEqual, 2)
			So(retrievedVersions, ShouldHaveLength, 2)
			So(retrievedVersions[0].ID, ShouldEqual, "version2")
			So(retrievedVersions[1].ID, ShouldEqual, "version1")
		})

		Convey("When GetAllStaticVersions is called with pagination (offset=1, limit=1)", func() {
			retrievedVersions, count, err := mongoStore.GetAllStaticVersions(ctx, staticDatasetID, "", 1, 1)

			So(err, ShouldBeNil)
			So(count, ShouldEqual, 2)
			So(retrievedVersions, ShouldHaveLength, 1)
			So(retrievedVersions[0].ID, ShouldEqual, "version1")
		})

		Convey("When GetAllStaticVersions is called with a limit only (limit=1)", func() {
			retrievedVersions, count, err := mongoStore.GetAllStaticVersions(ctx, staticDatasetID, "", 0, 1)

			So(err, ShouldBeNil)
			So(count, ShouldEqual, 2)
			So(retrievedVersions, ShouldHaveLength, 1)

			So(retrievedVersions[0].ID, ShouldEqual, "version2")
		})

		Convey("When GetAllStaticVersions is called with a non-matching datasetID", func() {
			retrievedVersions, count, err := mongoStore.GetAllStaticVersions(ctx, nonExistentDatasetID, "", 0, 0)

			So(err, ShouldEqual, errs.ErrVersionNotFound)
			So(count, ShouldEqual, 0)
			So(retrievedVersions, ShouldBeNil)
		})
	})
}
