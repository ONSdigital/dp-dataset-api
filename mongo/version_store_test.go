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

		mongoDB, err := getTestMongoDB(ctx, t)
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
	Convey("Given static versions are retrieved", t, func() {
		ctx := context.Background()

		mongoDB, err := getTestMongoDB(ctx, t)
		So(err, ShouldBeNil)

		_, err = setupVersionsTestData(ctx, mongoDB)
		So(err, ShouldBeNil)

		Convey("When GetStaticVersion is called with no published versions to be retrieved", func() {
			version, count, err := mongoDB.GetStaticVersionsByState(ctx, "", "0", 0, 20)

			Convey("Then the version is retrieved successfully", func() {
				So(err, ShouldBeNil)
				So(version, ShouldNotBeNil)
				So(count, ShouldEqual, 3)
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

func TestVersionsStatic(t *testing.T) {
	Convey("Given MongoDB is running and populated with static versions", t, func() {
		ctx := context.Background()
		mongoDB, err := getTestMongoDB(ctx, t)
		So(err, ShouldBeNil)

		_, err = setupVersionsTestData(ctx, mongoDB)
		So(err, ShouldBeNil)
		Convey("When GetVersionsStatic is called with no state, an approved version is returned", func() {
			retrievedVersions, count, err := mongoDB.GetVersionsStatic(ctx, staticDatasetID2, "neweditionapproved", "", 0, 20)

			So(err, ShouldBeNil)
			So(count, ShouldEqual, 1)
			So(retrievedVersions, ShouldHaveLength, 1)

			So(retrievedVersions[0].State, ShouldEqual, models.ApprovedState)
		})
	})
}

func TestGetVersionsStaticNoLimit(t *testing.T) {
	Convey("Given MongoDB is running and populated with static versions", t, func() {
		ctx := context.Background()
		mongoDB, err := getTestMongoDB(ctx, t)
		So(err, ShouldBeNil)

		_, err = setupVersionsTestData(ctx, mongoDB)
		So(err, ShouldBeNil)

		Convey("When GetVersionsStaticNoLimit is called with no state", func() {
			retrievedVersions, count, err := mongoDB.GetVersionsStaticNoLimit(ctx, staticDatasetID, "")

			Convey("Then all versions are returned", func() {
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 3)
				So(retrievedVersions, ShouldHaveLength, 3)
			})
		})

		Convey("When GetVersionsStaticNoLimit is called with a state filter", func() {
			retrievedVersions, count, err := mongoDB.GetVersionsStaticNoLimit(ctx, staticDatasetID, models.PublishedState)

			Convey("Then only versions matching the state are returned", func() {
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 1)
				So(retrievedVersions, ShouldHaveLength, 1)
				So(retrievedVersions[0].State, ShouldEqual, models.PublishedState)
			})
		})

		Convey("When GetVersionsStaticNoLimit and there no matching versions", func() {
			retrievedVersions, count, err := mongoDB.GetVersionsStaticNoLimit(ctx, nonExistentDatasetID, "")

			Convey("Then ErrVersionsNotFound is returned", func() {
				So(err, ShouldEqual, errs.ErrVersionsNotFound)
				So(count, ShouldEqual, 0)
				So(retrievedVersions, ShouldBeNil)
			})
		})

		Convey("When GetVersionsStaticNoLimit is called and the mongo connection fails", func() {
			err = mongoDB.Connection.Close(ctx)
			So(err, ShouldBeNil)

			retrievedVersions, count, err := mongoDB.GetVersionsStaticNoLimit(ctx, staticDatasetID, "")

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(count, ShouldEqual, 0)
				So(retrievedVersions, ShouldBeNil)
			})
		})
	})
}

func TestGetVersionStaticByPreviousEditionID(t *testing.T) {
	Convey("Given MongoDB is running with a version that has a previous edition ID", t, func() {
		ctx := context.Background()
		mongoDB, err := getTestMongoDB(ctx, t)
		So(err, ShouldBeNil)

		err = mongoDB.Connection.DropDatabase(ctx)
		So(err, ShouldBeNil)

		version := &models.Version{
			Version: 1,
			Edition: "renamed-edition",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{ID: staticDatasetID},
			},
			PreviousEditionId: []string{"previous-edition-id", "another-previous-edition-id"},
		}

		_, err = mongoDB.Connection.Collection(mongoDB.ActualCollectionName(config.VersionsCollection)).InsertOne(ctx, version)
		So(err, ShouldBeNil)

		Convey("When a matching previous edition ID is provided", func() {
			retrievedVersion, err := mongoDB.GetVersionStaticByPreviousEditionID(ctx, staticDatasetID, "previous-edition-id", 1)

			Convey("Then the correct version is returned", func() {
				So(err, ShouldBeNil)
				So(retrievedVersion, ShouldResemble, version)
			})
		})

		Convey("When a non-matching previous edition ID is provided", func() {
			_, err := mongoDB.GetVersionStaticByPreviousEditionID(ctx, staticDatasetID, "non-existent-previous-edition-id", 1)

			Convey("Then a VersionNotFound error is returned", func() {
				So(err, ShouldEqual, errs.ErrVersionNotFound)
			})
		})

		Convey("When the MongoDB connection fails", func() {
			err = mongoDB.Connection.Close(ctx)
			So(err, ShouldBeNil)

			_, err := mongoDB.GetVersionStaticByPreviousEditionID(ctx, staticDatasetID, "previous-edition-id", 1)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
			})
		})
	})
}

func TestGetVersionsStaticByEditionNoLimit(t *testing.T) {
	Convey("Given MongoDB is running and populated with static versions", t, func() {
		ctx := context.Background()
		mongoDB, err := getTestMongoDB(ctx, t)
		So(err, ShouldBeNil)

		_, err = setupVersionsTestData(ctx, mongoDB)
		So(err, ShouldBeNil)

		Convey("When GetVersionsStaticByEditionNoLimit is called with no state filter", func() {
			retrievedVersions, count, err := mongoDB.GetVersionsStaticByEditionNoLimit(ctx, staticDatasetID, "edition2", "")

			Convey("Then all versions for that edition are returned", func() {
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 2)
				So(retrievedVersions, ShouldHaveLength, 2)
				for _, v := range retrievedVersions {
					So(v.Edition, ShouldEqual, "edition2")
				}
			})
		})

		Convey("When GetVersionsStaticByEditionNoLimit is called with a state filter", func() {
			retrievedVersions, count, err := mongoDB.GetVersionsStaticByEditionNoLimit(ctx, staticDatasetID, "edition2", models.AssociatedState)

			Convey("Then only versions matching that state are returned", func() {
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 1)
				So(retrievedVersions, ShouldHaveLength, 1)
				So(retrievedVersions[0].State, ShouldEqual, models.AssociatedState)
				So(retrievedVersions[0].Edition, ShouldEqual, "edition2")
			})
		})

		Convey("When GetVersionsStaticByEditionNoLimit is called with a non-existent edition", func() {
			retrievedVersions, count, err := mongoDB.GetVersionsStaticByEditionNoLimit(ctx, staticDatasetID, "non-existent-edition", "")

			Convey("Then ErrVersionsNotFound is returned", func() {
				So(err, ShouldEqual, errs.ErrVersionsNotFound)
				So(count, ShouldEqual, 0)
				So(retrievedVersions, ShouldBeNil)
			})
		})

		Convey("When GetVersionsStaticByEditionNoLimit is called with a non-existent dataset", func() {
			retrievedVersions, count, err := mongoDB.GetVersionsStaticByEditionNoLimit(ctx, nonExistentDatasetID, "edition1", "")

			Convey("Then ErrVersionsNotFound is returned", func() {
				So(err, ShouldEqual, errs.ErrVersionsNotFound)
				So(count, ShouldEqual, 0)
				So(retrievedVersions, ShouldBeNil)
			})
		})

		Convey("When GetVersionsStaticByEditionNoLimit is called and the mongo connection fails", func() {
			err = mongoDB.Connection.Close(ctx)
			So(err, ShouldBeNil)

			retrievedVersions, count, err := mongoDB.GetVersionsStaticByEditionNoLimit(ctx, staticDatasetID, "edition2", "")

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(count, ShouldEqual, 0)
				So(retrievedVersions, ShouldBeNil)
			})
		})
	})
}

func TestGetAllStaticVersions(t *testing.T) {
	Convey("Given MongoDB is running and populated with static versions", t, func() {
		ctx := context.Background()
		mongoStore, err := getTestMongoDB(ctx, t)
		So(err, ShouldBeNil)

		versions, err := setupVersionsTestData(ctx, mongoStore)
		So(err, ShouldBeNil)
		So(versions, ShouldNotBeEmpty)

		// limit 0 only returns total count but no results
		Convey("When GetAllStaticVersions is called with offset=0 and limit=0", func() {
			retrievedVersions, count, err := mongoStore.GetAllStaticVersions(ctx, staticDatasetID, "", 0, 0)

			So(err, ShouldBeNil)
			So(count, ShouldEqual, 3)
			So(retrievedVersions, ShouldHaveLength, 0)
		})

		Convey("When GetAllStaticVersions is called with pagination (offset=1, limit=1)", func() {
			retrievedVersions, count, err := mongoStore.GetAllStaticVersions(ctx, staticDatasetID, "", 1, 1)

			So(err, ShouldBeNil)
			So(count, ShouldEqual, 3)
			So(retrievedVersions, ShouldHaveLength, 1)
			So(retrievedVersions[0].ID, ShouldEqual, "version1")
		})

		Convey("When GetAllStaticVersions is called with a limit only (limit=1)", func() {
			retrievedVersions, count, err := mongoStore.GetAllStaticVersions(ctx, staticDatasetID, "", 0, 1)

			So(err, ShouldBeNil)
			So(count, ShouldEqual, 3)
			So(retrievedVersions, ShouldHaveLength, 1)

			So(retrievedVersions[0].ID, ShouldEqual, "version2")
		})

		Convey("When GetAllStaticVersions is called with a non-matching datasetID", func() {
			retrievedVersions, count, err := mongoStore.GetAllStaticVersions(ctx, nonExistentDatasetID, "", 0, 0)

			So(err, ShouldEqual, errs.ErrVersionsNotFound)
			So(count, ShouldEqual, 0)
			So(retrievedVersions, ShouldBeNil)
		})
	})
}

func TestGetEditionsStatic(t *testing.T) {
	Convey("Given MongoDB is running and populated with static versions", t, func() {
		ctx := context.Background()
		mongoStore, err := getTestMongoDB(ctx, t)
		So(err, ShouldBeNil)

		versions, err := setupVersionsTestData(ctx, mongoStore)
		So(err, ShouldBeNil)
		So(versions, ShouldNotBeEmpty)

		Convey("When GetEditionsStatic is called with no state filter", func() {
			retrievedEditions, count, err := mongoStore.GetEditionsStatic(ctx, staticDatasetID, "", 0, 20)

			Convey("Then it returns the expected total number of unique editions", func() {
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 2)
				So(retrievedEditions, ShouldHaveLength, 2)
			})

			Convey("And the editions are ordered by version 1 release date in descending order", func() {
				So(retrievedEditions[0].Next.Edition, ShouldEqual, "edition1")
				So(retrievedEditions[1].Next.Edition, ShouldEqual, "edition2")
			})

			Convey("And each edition is mapped to the latest published and unpublished versions correctly", func() {
				So(retrievedEditions[0].Current.Edition, ShouldEqual, "edition1")
				So(retrievedEditions[0].Current.Version, ShouldEqual, 1)
				So(retrievedEditions[0].Next.Edition, ShouldEqual, "edition1")
				So(retrievedEditions[0].Next.Version, ShouldEqual, 1)

				So(retrievedEditions[1].Current, ShouldBeNil)
				So(retrievedEditions[1].Next.Edition, ShouldEqual, "edition2")
				So(retrievedEditions[1].Next.Version, ShouldEqual, 2)
			})
		})

		Convey("When GetEditionsStatic is called with pagination", func() {
			retrievedEditions, count, err := mongoStore.GetEditionsStatic(ctx, staticDatasetID, "", 1, 1)

			Convey("Then it returns a paginated subset while preserving the total edition count", func() {
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 2)
				So(retrievedEditions, ShouldHaveLength, 1)
				So(retrievedEditions[0].Current, ShouldBeNil)
				So(retrievedEditions[0].Next.Edition, ShouldEqual, "edition2")
				So(retrievedEditions[0].Next.Version, ShouldEqual, 2)
			})
		})

		Convey("When GetEditionsStatic is called with the published state", func() {
			retrievedEditions, count, err := mongoStore.GetEditionsStatic(ctx, staticDatasetID, models.PublishedState, 0, 20)

			Convey("Then it only returns editions that have published versions", func() {
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 1)
				So(retrievedEditions, ShouldHaveLength, 1)
				So(retrievedEditions[0].Current.Edition, ShouldEqual, "edition1")
				So(retrievedEditions[0].Current.Version, ShouldEqual, 1)
				So(retrievedEditions[0].Next.Edition, ShouldEqual, "edition1")
				So(retrievedEditions[0].Next.Version, ShouldEqual, 1)
			})
		})

		Convey("When GetEditionsStatic is called with a non-existent datasetID", func() {
			retrievedEditions, count, err := mongoStore.GetEditionsStatic(ctx, nonExistentDatasetID, "", 0, 20)

			Convey("Then ErrEditionsNotFound is returned", func() {
				So(err, ShouldEqual, errs.ErrEditionsNotFound)
				So(count, ShouldEqual, 0)
				So(retrievedEditions, ShouldBeNil)
			})
		})
	})
}

func TestCheckVersionExistsStatic(t *testing.T) {
	Convey("Given MongoDB is running with static versions", t, func() {
		ctx := context.Background()
		mongo, err := getTestMongoDB(ctx, t)
		So(err, ShouldBeNil)

		versions, err := setupVersionsTestData(ctx, mongo)
		So(err, ShouldBeNil)
		So(versions, ShouldNotBeEmpty)

		Convey("When CheckVersionExistsStatic is called for an existing version", func() {
			exists, err := mongo.CheckVersionExistsStatic(ctx, staticDatasetID, "edition1", 1)
			Convey("Then it returns true with no error", func() {
				So(err, ShouldBeNil)
				So(exists, ShouldBeTrue)
			})
		})

		Convey("When CheckVersionExistsStatic is called for a non-existing version", func() {
			exists, err := mongo.CheckVersionExistsStatic(ctx, staticDatasetID, "edition1", 99)
			Convey("Then it returns false with no error", func() {
				So(err, ShouldBeNil)
				So(exists, ShouldBeFalse)
			})
		})

		Convey("When CheckVersionExistsStatic is called and the mongo connection fails", func() {
			err = mongo.Connection.Close(ctx)
			So(err, ShouldBeNil)

			exists, err := mongo.CheckVersionExistsStatic(ctx, staticDatasetID, "edition1", 1)
			Convey("Then it returns an error", func() {
				So(err, ShouldNotBeNil)
				So(exists, ShouldBeFalse)
			})
		})
	})
}

func TestDeleteStaticDatasetVersion(t *testing.T) {
	Convey("Given MongoDB is running", t, func() {
		ctx := context.Background()

		Convey("When DeleteStaticDatasetVersion is called with a matching dataset, edition and unpublished version", func() {
			mongoStore, err := getTestMongoDB(ctx, t)
			So(err, ShouldBeNil)

			versions, err := setupVersionsTestData(ctx, mongoStore)
			So(err, ShouldBeNil)
			So(versions, ShouldHaveLength, 5)

			datasetToDelete := staticDatasetID
			editionToDelete := "edition2"
			versionToDelete := 2
			err = mongoStore.DeleteStaticDatasetVersion(ctx, datasetToDelete, editionToDelete, versionToDelete)

			So(err, ShouldBeNil)
			selector := bson.M{"links.dataset.id": staticDatasetID}
			totalCount, err := mongoStore.Connection.Collection(mongoStore.ActualCollectionName(config.VersionsCollection)).Count(ctx, selector)
			So(err, ShouldBeNil)
			So(totalCount, ShouldEqual, 2)
		})
	})
}

func TestCheckEditionTitleExistsStatic(t *testing.T) {
	Convey("Given MongoDB is running with static versions", t, func() {
		ctx := context.Background()
		mongo, err := getTestMongoDB(ctx, t)
		So(err, ShouldBeNil)

		versions, err := setupVersionsTestData(ctx, mongo)
		So(err, ShouldBeNil)
		So(versions, ShouldNotBeEmpty)

		Convey("When CheckEditionTitleExistsStatic is called with an existing edition title", func() {
			err := mongo.CheckEditionTitleExistsStatic(ctx, staticDatasetID, "First Edition")

			Convey("Then it returns ErrEditionTitleAlreadyExists", func() {
				So(err, ShouldEqual, errs.ErrEditionTitleAlreadyExists)
			})
		})

		Convey("When CheckEditionTitleExistsStatic is called with non-existing edition title", func() {
			err := mongo.CheckEditionTitleExistsStatic(ctx, staticDatasetID, "New Title")

			Convey("Then it returns nil (no conflict)", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When CheckEditionTitleExistsStatic is called with existing edition title but for a different dataset", func() {
			err := mongo.CheckEditionTitleExistsStatic(ctx, "different-dataset-id", "First Edition")

			Convey("Then it returns nil (no conflict across datasets)", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}
