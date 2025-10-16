package mongo

import (
	"context"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
)

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

func TestGetStaticVersionsByState(t *testing.T) {
	Convey("Given a static versions are retieved", t, func() {
		ctx := context.Background()

		mongoDB, _, err := getTestMongoDB(ctx)
		So(err, ShouldBeNil)

		_, err = setupVersionsTestData(ctx, mongoDB)
		So(err, ShouldBeNil)

		Convey("When GetStaticVersion is called with no published versions to be retrieved", func() {
			version, count, err := mongoDB.GetStaticVersionsByState(ctx, "", "0", 0, 20)

			Convey("Then the version is retrieved successfully", func() {
				So(err, ShouldBeNil)
				So(version, ShouldNotBeNil)
				So(count, ShouldEqual, 1)
				So(version[0].State, ShouldNotEqual, models.PublishedState)
			})
		})

		Convey("When GetStaticVersion is called with only published versions to be retrieved", func() {
			version, count, err := mongoDB.GetStaticVersionsByState(ctx, "", "TRUE", 0, 20)

			Convey("Then the version is retrieved successfully", func() {
				So(err, ShouldBeNil)
				So(version, ShouldNotBeNil)
				So(count, ShouldEqual, 2)
				So(version[0].State, ShouldEqual, models.PublishedState)
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

		versions, err := setupVersionsTestData(ctx, mongoStore)
		So(err, ShouldBeNil)
		So(versions, ShouldNotBeEmpty)

		Convey("When GetAllStaticVersions is called with offset=0 and limit=0", func() {
			_, count, err := mongoStore.GetAllStaticVersions(ctx, staticDatasetID, "", 0, 0)

			So(err, ShouldBeNil)
			So(count, ShouldEqual, 2)
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

func TestCheckVersionExistsStatic(t *testing.T) {
	Convey("Given a MongoDB instance with static versions", t, func() {
		ctx := context.Background()
		mongo, _, err := getTestMongoDB(ctx)
		So(err, ShouldBeNil)

		versions, err := setupVersionsTestData(ctx, mongo)
		So(err, ShouldBeNil)
		So(versions, ShouldNotBeEmpty)

		Convey("When CheckVersionExistsStatic is called for an existing version", func() {
			exists, err := mongo.CheckVersionExistsStatic(ctx, staticDatasetID, "edition1", 1, "")
			Convey("Then it returns true with no error", func() {
				So(err, ShouldBeNil)
				So(exists, ShouldBeTrue)
			})
		})

		Convey("When CheckVersionExistsStatic is called for a non-existing version", func() {
			exists, err := mongo.CheckVersionExistsStatic(ctx, staticDatasetID, "edition1", 99, "")
			Convey("Then it returns false with no error", func() {
				So(err, ShouldBeNil)
				So(exists, ShouldBeFalse)
			})
		})

		Convey("When CheckVersionExistsStatic is called for an existing version with a specific state", func() {
			exists, err := mongo.CheckVersionExistsStatic(ctx, staticDatasetID, "edition1", 1, "published")
			Convey("Then it returns true with no error", func() {
				So(err, ShouldBeNil)
				So(exists, ShouldBeTrue)
			})
		})

		Convey("When CheckVersionExistsStatic is called for an existing version with a non-matching state", func() {
			exists, err := mongo.CheckVersionExistsStatic(ctx, staticDatasetID, "edition1", 1, "stateThatDoesNotMatch")
			Convey("Then it returns false with no error", func() {
				So(err, ShouldBeNil)
				So(exists, ShouldBeFalse)
			})
		})

		Convey("When CheckVersionExistsStatic is called and the mongo connection fails", func() {
			err = mongo.Connection.Close(ctx)
			So(err, ShouldBeNil)

			exists, err := mongo.CheckVersionExistsStatic(ctx, staticDatasetID, "edition1", 1, "")
			Convey("Then it returns an error", func() {
				So(err, ShouldNotBeNil)
				So(exists, ShouldBeFalse)
			})
		})
	})
}
