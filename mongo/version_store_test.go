package mongo

import (
	"context"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
	"go.mongodb.org/mongo-driver/bson"
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
	Convey("Given an in-memory MongoDB is running and populated with static versions", t, func() {
		ctx := context.Background()

		mongoDB, _, err := getTestMongoDB(ctx)
		So(err, ShouldBeNil)

		existingVersions, err := setupVersionsTestData(ctx, mongoDB)
		So(err, ShouldBeNil)

		Convey("When GetStaticVersionsByState is called without a state", func() {
			versions, count, err := mongoDB.GetStaticVersionsByState(ctx, "", 0, 20)

			Convey("Then all versions are retrieved successfully regardless of state", func() {
				So(err, ShouldBeNil)
				So(versions, ShouldNotBeNil)
				So(count, ShouldEqual, len(existingVersions))
			})
		})

		Convey("When GetStaticVersionsByState is called with a specific state", func() {
			versions, count, err := mongoDB.GetStaticVersionsByState(ctx, models.EditionConfirmedState, 0, 20)

			Convey("Then all versions with the specified state are retrieved successfully", func() {
				So(err, ShouldBeNil)
				So(versions, ShouldNotBeNil)
				So(count, ShouldEqual, 1)
				So(versions[0].State, ShouldEqual, models.EditionConfirmedState)
			})
		})

		Convey("When GetStaticVersionsByState is called and there are no matching versions", func() {
			versions, count, err := mongoDB.GetStaticVersionsByState(ctx, "nonexistentstate", 0, 20)

			Convey("Then a VersionsNotFound error is returned", func() {
				So(err, ShouldEqual, errs.ErrVersionsNotFound)
				So(versions, ShouldBeNil)
				So(count, ShouldEqual, 0)
			})
		})
	})
}

func TestGetStaticVersionsByPublishedState(t *testing.T) {
	Convey("Given an in-memory MongoDB is running and populated with static versions", t, func() {
		ctx := context.Background()

		mongoDB, _, err := getTestMongoDB(ctx)
		So(err, ShouldBeNil)

		_, err = setupVersionsTestData(ctx, mongoDB)
		So(err, ShouldBeNil)

		Convey("When GetStaticVersionsByPublishedState is called with isPublished=true", func() {
			versions, count, err := mongoDB.GetStaticVersionsByPublishedState(ctx, true, 0, 20)

			Convey("Then all published versions are retrieved successfully", func() {
				So(err, ShouldBeNil)
				So(versions, ShouldNotBeNil)
				So(count, ShouldEqual, 2)
				for _, v := range versions {
					So(v.State, ShouldEqual, models.PublishedState)
				}
			})
		})

		Convey("When GetStaticVersionsByPublishedState is called with isPublished=false", func() {
			versions, count, err := mongoDB.GetStaticVersionsByPublishedState(ctx, false, 0, 20)

			Convey("Then all unpublished versions are retrieved successfully", func() {
				So(err, ShouldBeNil)
				So(versions, ShouldNotBeNil)
				So(count, ShouldEqual, 1)
				for _, v := range versions {
					So(v.State, ShouldNotEqual, models.PublishedState)
				}
			})
		})
	})

	Convey("Given an in-memory MongoDB is running and has no static versions", t, func() {
		ctx := context.Background()

		mongoDB, _, err := getTestMongoDB(ctx)
		So(err, ShouldBeNil)

		Convey("When GetStaticVersionsByPublishedState is called with isPublished=true", func() {
			versions, count, err := mongoDB.GetStaticVersionsByPublishedState(ctx, true, 0, 20)

			Convey("Then a VersionsNotFound error is returned", func() {
				So(err, ShouldEqual, errs.ErrVersionsNotFound)
				So(versions, ShouldBeNil)
				So(count, ShouldEqual, 0)
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

func TestDeleteStaticVersionsByDatasetID(t *testing.T) {
	Convey("Given an in-memory MongoDB is running", t, func() {
		ctx := context.Background()

		Convey("When DeleteStaticVersionsByDatasetID is called with a matching datasetID", func() {
			mongoStore, _, err := getTestMongoDB(ctx)
			So(err, ShouldBeNil)

			versions, err := setupVersionsTestData(ctx, mongoStore)
			So(err, ShouldBeNil)
			So(versions, ShouldHaveLength, 3)

			deletedCount, err := mongoStore.DeleteStaticVersionsByDatasetID(ctx, staticDatasetID)
			So(err, ShouldBeNil)
			So(deletedCount, ShouldEqual, 2)

			selector := bson.M{"links.dataset.id": staticDatasetID}
			totalCount, err := mongoStore.Connection.Collection(mongoStore.ActualCollectionName(config.VersionsCollection)).Count(ctx, selector)
			So(err, ShouldBeNil)
			So(totalCount, ShouldEqual, 0)
		})

		Convey("When DeleteStaticVersionsByDatasetID is called for a dataset with no versions", func() {
			mongoStore, _, err := getTestMongoDB(ctx)
			So(err, ShouldBeNil)

			deletedCount, err := mongoStore.DeleteStaticVersionsByDatasetID(ctx, nonExistentDatasetID)
			So(err, ShouldEqual, errs.ErrVersionsNotFound)
			So(deletedCount, ShouldEqual, 0)
		})
	})
}
