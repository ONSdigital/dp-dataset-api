package application

import (
	"context"
	"errors"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testDatasetID = "test-dataset-id"
	testEditionID = "test-edition-id"
	testVersionID = 1

	errDataStoreFailure = errors.New("data store failure")
)

func TestGetVersionPublic(t *testing.T) {
	Convey("Given a static dataset service with a mocked data store", t, func() {
		mockDataStore := &storetest.StorerMock{}
		staticDatasetService := NewStaticDatasetService(store.DataStore{Backend: mockDataStore})
		ctx := context.Background()

		Convey("When the dataset is static, edition exists and version exists", func() {
			mockDataStore.GetDatasetTypeFunc = func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				So(authorised, ShouldBeFalse)
				return models.Static.String(), nil
			}
			mockDataStore.CheckEditionExistsStaticFunc = func(ctx context.Context, datasetID, editionID string, state string) error {
				So(state, ShouldEqual, models.PublishedState)
				return nil
			}
			mockDataStore.GetVersionStaticFunc = func(ctx context.Context, datasetID string, editionID string, version int, state string) (*models.Version, error) {
				So(state, ShouldEqual, models.PublishedState)
				return &models.Version{
					IsMigration:       new(bool),
					PreviousEditionId: []string{"previous-edition-id"},
				}, nil
			}

			Convey("Then the version is returned with private fields redacted", func() {
				version, err := staticDatasetService.GetVersionPublic(ctx, testDatasetID, testEditionID, testVersionID)
				So(err, ShouldBeNil)
				So(version, ShouldNotBeNil)
				So(version.IsMigration, ShouldBeNil)
				So(version.PreviousEditionId, ShouldBeNil)

				So(len(mockDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
				So(len(mockDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
				So(len(mockDataStore.GetVersionStaticCalls()), ShouldEqual, 1)
			})
		})

		Convey("When GetDatasetType returns an error", func() {
			mockDataStore.GetDatasetTypeFunc = func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return "", errDataStoreFailure
			}

			Convey("Then the error is returned", func() {
				version, err := staticDatasetService.GetVersionPublic(ctx, testDatasetID, testEditionID, testVersionID)
				So(err, ShouldEqual, errDataStoreFailure)
				So(version, ShouldBeNil)

				So(len(mockDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
			})
		})

		Convey("When the dataset is not static", func() {
			mockDataStore.GetDatasetTypeFunc = func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return "non-static", nil
			}

			Convey("Then an ErrDatasetNotStatic error is returned", func() {
				version, err := staticDatasetService.GetVersionPublic(ctx, testDatasetID, testEditionID, testVersionID)
				So(err, ShouldEqual, apierrors.ErrDatasetNotStatic)
				So(version, ShouldBeNil)

				So(len(mockDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
			})
		})

		Convey("When the edition does not exist", func() {
			mockDataStore.GetDatasetTypeFunc = func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return models.Static.String(), nil
			}
			mockDataStore.CheckEditionExistsStaticFunc = func(ctx context.Context, datasetID, editionID string, state string) error {
				return apierrors.ErrEditionNotFound
			}

			Convey("Then an ErrEditionNotFound error is returned", func() {
				version, err := staticDatasetService.GetVersionPublic(ctx, testDatasetID, testEditionID, testVersionID)
				So(err, ShouldEqual, apierrors.ErrEditionNotFound)
				So(version, ShouldBeNil)

				So(len(mockDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
				So(len(mockDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
			})
		})

		Convey("When the version does not exist", func() {
			mockDataStore.GetDatasetTypeFunc = func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return models.Static.String(), nil
			}
			mockDataStore.CheckEditionExistsStaticFunc = func(ctx context.Context, datasetID, editionID string, state string) error {
				return nil
			}
			mockDataStore.GetVersionStaticFunc = func(ctx context.Context, datasetID string, editionID string, version int, state string) (*models.Version, error) {
				return nil, apierrors.ErrVersionNotFound
			}

			Convey("Then an ErrVersionNotFound error is returned", func() {
				version, err := staticDatasetService.GetVersionPublic(ctx, testDatasetID, testEditionID, testVersionID)
				So(err, ShouldEqual, apierrors.ErrVersionNotFound)
				So(version, ShouldBeNil)

				So(len(mockDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
				So(len(mockDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
				So(len(mockDataStore.GetVersionStaticCalls()), ShouldEqual, 1)
			})
		})
	})
}

func TestGetVersionPrivate(t *testing.T) {
	Convey("Given a static dataset service with a mocked data store", t, func() {
		mockDataStore := &storetest.StorerMock{}
		staticDatasetService := NewStaticDatasetService(store.DataStore{Backend: mockDataStore})
		ctx := context.Background()

		Convey("When the dataset is static, edition exists and version exists", func() {
			mockDataStore.GetDatasetTypeFunc = func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				So(authorised, ShouldBeTrue)
				return models.Static.String(), nil
			}
			mockDataStore.CheckEditionExistsStaticFunc = func(ctx context.Context, datasetID, editionID string, state string) error {
				So(state, ShouldEqual, "")
				return nil
			}
			mockDataStore.GetVersionStaticFunc = func(ctx context.Context, datasetID string, editionID string, version int, state string) (*models.Version, error) {
				So(state, ShouldEqual, "")
				return &models.Version{
					Edition: "direct-match-edition-id",
				}, nil
			}

			Convey("Then the version is returned", func() {
				version, err := staticDatasetService.GetVersionPrivate(ctx, testDatasetID, testEditionID, testVersionID)
				So(err, ShouldBeNil)
				So(version, ShouldNotBeNil)
				So(version.Edition, ShouldEqual, "direct-match-edition-id")

				So(len(mockDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
				So(len(mockDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
				So(len(mockDataStore.GetVersionStaticCalls()), ShouldEqual, 1)
			})
		})

		Convey("When the editionID is not a direct match but matches a previous edition ID", func() {
			mockDataStore.GetDatasetTypeFunc = func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				So(authorised, ShouldBeTrue)
				return models.Static.String(), nil
			}
			mockDataStore.CheckEditionExistsStaticFunc = func(ctx context.Context, datasetID, editionID string, state string) error {
				So(state, ShouldEqual, "")
				return apierrors.ErrEditionNotFound
			}
			mockDataStore.GetVersionStaticByPreviousEditionIDFunc = func(ctx context.Context, datasetID string, previousEditionID string, versionID int) (*models.Version, error) {
				So(previousEditionID, ShouldEqual, testEditionID)
				return &models.Version{
					Edition:           "renamed-edition-id",
					PreviousEditionId: []string{testEditionID},
				}, nil
			}

			Convey("Then the version is returned", func() {
				version, err := staticDatasetService.GetVersionPrivate(ctx, testDatasetID, testEditionID, testVersionID)
				So(err, ShouldBeNil)
				So(version, ShouldNotBeNil)
				So(version.Edition, ShouldEqual, "renamed-edition-id")

				So(len(mockDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
				So(len(mockDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
				So(len(mockDataStore.GetVersionStaticByPreviousEditionIDCalls()), ShouldEqual, 1)
			})
		})

		Convey("When GetDatasetType returns an error", func() {
			mockDataStore.GetDatasetTypeFunc = func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return "", errDataStoreFailure
			}

			Convey("Then the error is returned", func() {
				version, err := staticDatasetService.GetVersionPrivate(ctx, testDatasetID, testEditionID, testVersionID)
				So(err, ShouldEqual, errDataStoreFailure)
				So(version, ShouldBeNil)

				So(len(mockDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
			})
		})

		Convey("When the dataset is not static", func() {
			mockDataStore.GetDatasetTypeFunc = func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return "non-static", nil
			}

			Convey("Then an ErrDatasetNotStatic error is returned", func() {
				version, err := staticDatasetService.GetVersionPrivate(ctx, testDatasetID, testEditionID, testVersionID)
				So(err, ShouldEqual, apierrors.ErrDatasetNotStatic)
				So(version, ShouldBeNil)

				So(len(mockDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
			})
		})

		Convey("When GetVersionStatic returns an unexpected error", func() {
			mockDataStore.GetDatasetTypeFunc = func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				So(authorised, ShouldBeTrue)
				return models.Static.String(), nil
			}
			mockDataStore.CheckEditionExistsStaticFunc = func(ctx context.Context, datasetID, editionID string, state string) error {
				So(state, ShouldEqual, "")
				return nil
			}
			mockDataStore.GetVersionStaticFunc = func(ctx context.Context, datasetID string, editionID string, version int, state string) (*models.Version, error) {
				So(state, ShouldEqual, "")
				return nil, errDataStoreFailure
			}

			Convey("Then the error is returned", func() {
				version, err := staticDatasetService.GetVersionPrivate(ctx, testDatasetID, testEditionID, testVersionID)
				So(err, ShouldEqual, errDataStoreFailure)
				So(version, ShouldBeNil)

				So(len(mockDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
				So(len(mockDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
				So(len(mockDataStore.GetVersionStaticCalls()), ShouldEqual, 1)
			})
		})

		Convey("When CheckEditionExistsStatic returns an unexpected error", func() {
			mockDataStore.GetDatasetTypeFunc = func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				So(authorised, ShouldBeTrue)
				return models.Static.String(), nil
			}
			mockDataStore.CheckEditionExistsStaticFunc = func(ctx context.Context, datasetID, editionID string, state string) error {
				So(state, ShouldEqual, "")
				return errDataStoreFailure
			}

			Convey("Then the error is returned", func() {
				version, err := staticDatasetService.GetVersionPrivate(ctx, testDatasetID, testEditionID, testVersionID)
				So(err, ShouldEqual, errDataStoreFailure)
				So(version, ShouldBeNil)

				So(len(mockDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
				So(len(mockDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
			})
		})

		Convey("When the editionID is not a direct match and does not match a previous edition ID", func() {
			mockDataStore.GetDatasetTypeFunc = func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				So(authorised, ShouldBeTrue)
				return models.Static.String(), nil
			}
			mockDataStore.CheckEditionExistsStaticFunc = func(ctx context.Context, datasetID, editionID string, state string) error {
				So(state, ShouldEqual, "")
				return apierrors.ErrEditionNotFound
			}
			mockDataStore.GetVersionStaticByPreviousEditionIDFunc = func(ctx context.Context, datasetID string, previousEditionID string, versionID int) (*models.Version, error) {
				So(previousEditionID, ShouldEqual, testEditionID)
				return nil, apierrors.ErrVersionNotFound
			}

			Convey("Then an ErrEditionNotFound error is returned", func() {
				version, err := staticDatasetService.GetVersionPrivate(ctx, testDatasetID, testEditionID, testVersionID)
				So(err, ShouldEqual, apierrors.ErrEditionNotFound)
				So(version, ShouldBeNil)

				So(len(mockDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
				So(len(mockDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
				So(len(mockDataStore.GetVersionStaticByPreviousEditionIDCalls()), ShouldEqual, 1)
			})
		})

		Convey("When GetVersionStaticByPreviousEditionID returns an unexpected error", func() {
			mockDataStore.GetDatasetTypeFunc = func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				So(authorised, ShouldBeTrue)
				return models.Static.String(), nil
			}
			mockDataStore.CheckEditionExistsStaticFunc = func(ctx context.Context, datasetID, editionID string, state string) error {
				So(state, ShouldEqual, "")
				return apierrors.ErrEditionNotFound
			}
			mockDataStore.GetVersionStaticByPreviousEditionIDFunc = func(ctx context.Context, datasetID string, previousEditionID string, versionID int) (*models.Version, error) {
				So(previousEditionID, ShouldEqual, testEditionID)
				return nil, errDataStoreFailure
			}

			Convey("Then the error is returned", func() {
				version, err := staticDatasetService.GetVersionPrivate(ctx, testDatasetID, testEditionID, testVersionID)
				So(err, ShouldEqual, errDataStoreFailure)
				So(version, ShouldBeNil)

				So(len(mockDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
				So(len(mockDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
				So(len(mockDataStore.GetVersionStaticByPreviousEditionIDCalls()), ShouldEqual, 1)
			})
		})
	})
}
