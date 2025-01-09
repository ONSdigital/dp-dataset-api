package application

import (
	"context"
	"errors"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/mocks"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/smartystreets/goconvey/convey"
)

var testContext = context.Background()

var publishVersionUpdate = &models.Version{
	State:        models.PublishedState,
	ReleaseDate:  "2024-12-31",
	ID:           "a1b2c3",
	CollectionID: "3434",
}

var currentVersionEditionConfirmed = &models.Version{
	State:        models.EditionConfirmedState,
	CollectionID: "3434",
}

var versionDetails = VersionDetails{
	datasetID: "123",
	version:   "1",
	edition:   "2017",
}

var versionUpdateAssociated = &models.Version{
	State:        models.AssociatedState,
	ReleaseDate:  "2024-12-31",
	Version:      1,
	ID:           "789",
	CollectionID: "3434",
}

var versionUpdateEditionConfirmed = &models.Version{
	State:        models.EditionConfirmedState,
	ReleaseDate:  "2024-12-31",
	ID:           "789",
	CollectionID: "3434",
}

func setUpStatesTransitions() ([]State, []Transition) {
	states := []State{Published, EditionConfirmed, Associated}
	transitions := []Transition{{
		Label:                "published",
		TargetState:          Published,
		AlllowedSourceStates: []string{"associated", "published", "edition-confirmed"},
	}, {
		Label:                "associated",
		TargetState:          Associated,
		AlllowedSourceStates: []string{"edition-confirmed", "associated"},
	}, {
		Label:                "edition-confirmed",
		TargetState:          EditionConfirmed,
		AlllowedSourceStates: []string{"edition-confirmed", "completed", "published"},
	}}

	return states, transitions
}

func TestAmendVersionInvalidState(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version tries to transition from created to published", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					Type:  "null",
					State: models.CreatedState,
				}, nil
			},
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				return "", nil
			},
			UnlockInstanceFunc: func(context.Context, string) {},
		}

		vars := make(map[string]string)
		vars["dataset_id"] = "123"
		vars["edition"] = "2021"
		vars["version"] = "1"

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})
		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		err := smDS.AmendVersion(testContext, vars, publishVersionUpdate)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "State not allowed to transition")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 0)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.AcquireInstanceLockCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UnlockInstanceCalls()), convey.ShouldEqual, 1)
	})
}

func TestAmendVersionPopulateModelsFails(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version tries to transition from associated to published and the edition is not found", t, func() {
		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
			},
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				return "", nil
			},
			UnlockInstanceFunc: func(context.Context, string) {},
		}

		vars := make(map[string]string)
		vars["dataset_id"] = "123"
		vars["edition"] = "2021"
		vars["version"] = "1"

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})
		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		err := smDS.AmendVersion(testContext, vars, publishVersionUpdate)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "edition not found")
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.AcquireInstanceLockCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UnlockInstanceCalls()), convey.ShouldEqual, 1)
	})
}

func TestAmendVersionPopulateVersionFails(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version tries to transition from edition-confirmed to associated and the model is not valid", t, func() {
		versionUpdateInvalid := &models.Version{
			State:        models.AssociatedState,
			Version:      1,
			ID:           "789",
			CollectionID: "3434",
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				return "", nil
			},
			UnlockInstanceFunc: func(context.Context, string) {},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID: "789",
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
							ID:   "123",
						},
						Dimensions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017",
							ID:   "456",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
						},
					},
					State: models.EditionConfirmedState,
					ETag:  "12345",
				}, nil
			},
		}

		vars := make(map[string]string)
		vars["dataset_id"] = "123"
		vars["edition"] = "2021"
		vars["version"] = "1"

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})
		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		err := smDS.AmendVersion(testContext, vars, versionUpdateInvalid)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "missing mandatory fields: [release_date]")
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.AcquireInstanceLockCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UnlockInstanceCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
	})
}

func TestAmendVersionSuccess(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	vars := make(map[string]string)
	vars["dataset_id"] = "123"
	vars["edition"] = "2021"
	vars["version"] = "1"

	convey.Convey("When a request is made to change a version from associated to published but the dataset is not found", t, func() {
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			UpdateDatasetWithAssociationFunc: func(context.Context, string, string, *models.Version) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID: "789",
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
							ID:   "123",
						},
						Dimensions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017",
							ID:   "456",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
						},
					},
					ReleaseDate: "2017-12-12",
					State:       models.EditionConfirmedState,
					ETag:        "12345",
				}, nil
			},
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				return "", nil
			},
			UnlockInstanceFunc: func(context.Context, string) {},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})
		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		err := smDS.AmendVersion(testContext, vars, versionUpdateAssociated)
		convey.So(err, convey.ShouldBeNil)
		convey.So(len(mockedDataStore.AcquireInstanceLockCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UnlockInstanceCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 1)
	})
}

func TestAmendVersionErrorLockFails(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	vars := make(map[string]string)
	vars["dataset_id"] = "123"
	vars["edition"] = "2021"
	vars["version"] = "1"

	convey.Convey("When a request is made to change state from associated to published but the database lock fails", t, func() {
		mockedDataStore := &storetest.StorerMock{
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				return "", errors.New("Unable to acquire lock")
			},
			UnlockInstanceFunc: func(context.Context, string) {},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})
		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		err := smDS.AmendVersion(testContext, vars, publishVersionUpdate)

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "Unable to acquire lock")
	})
}

func TestAssociateVersionInvalidVersion(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	convey.Convey("When a version is set to associated from edition-confirmed and the version is invalid", t, func() {
		invalidVersionDetails := VersionDetails{
			datasetID: "123",
			version:   "bob",
			edition:   "2017",
		}

		mockedDataStore := &storetest.StorerMock{}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := AssociateVersion(testContext, smDS, currentVersionEditionConfirmed, versionUpdateAssociated, invalidVersionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "invalid version requested")
	})
}

func TestAssociateVersionInvalidType(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	convey.Convey("When a version is set to published from associated with an incorrect type", t, func() {
		currentVersion := &models.Version{
			State:        models.EditionConfirmedState,
			CollectionID: "3434",
			Type:         "not_a_type",
		}

		versionUpdate := &models.Version{
			State:        models.AssociatedState,
			ReleaseDate:  "2024-12-31",
			Version:      1,
			ID:           "789",
			CollectionID: "3434",
			Type:         "not_a_type",
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			UpdateDatasetWithAssociationFunc: func(context.Context, string, string, *models.Version) error {
				return nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := AssociateVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "error getting type of version: invalid dataset type")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 1)
	})
}

func TestAssociateVersionInvalidRequest(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	convey.Convey("When a version is set to associated from edition-confirmed and is missing a release date", t, func() {
		versionUpdate := &models.Version{
			State:        models.AssociatedState,
			Version:      1,
			ID:           "789",
			CollectionID: "3434",
		}

		mockedDataStore := &storetest.StorerMock{}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := AssociateVersion(testContext, smDS, currentVersionEditionConfirmed, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "missing mandatory fields: [release_date]")
	})
}

func TestAssociateVersionErrorDownloaderType(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	convey.Convey("When a version is set to associated from edition-confirmed for a type which does not generate downloads", t, func() {
		currentStaticVersion := &models.Version{
			State:        models.EditionConfirmedState,
			CollectionID: "3434",
			Type:         "static",
		}

		versionstaticUpdate := &models.Version{
			State:        models.AssociatedState,
			Version:      1,
			ReleaseDate:  "2024-05-23",
			ID:           "789",
			CollectionID: "3434",
			Type:         "static",
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			UpdateDatasetWithAssociationFunc: func(context.Context, string, string, *models.Version) error {
				return nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := AssociateVersion(testContext, smDS, currentStaticVersion, versionstaticUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "no downloader available for type")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 1)
	})
}

func TestAssociateVersionErrors(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	convey.Convey("When a version is set to associated from edition-confirmed but the dataset is not found", t, func() {
		versionUpdate := &models.Version{
			State:        models.AssociatedState,
			Version:      1,
			ReleaseDate:  "2024-05-23",
			ID:           "789",
			CollectionID: "3434",
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			UpdateDatasetWithAssociationFunc: func(context.Context, string, string, *models.Version) error {
				return errs.ErrDatasetNotFound
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := AssociateVersion(testContext, smDS, currentVersionEditionConfirmed, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "dataset not found")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 1)
	})
}

func TestAssociateVersionFailedToGenerateDownloads(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to published from associated and the downloads fail to generate", t, func() {
		currentVersion := &models.Version{
			State:        models.EditionConfirmedState,
			CollectionID: "3434",
			Type:         models.CantabularFlexibleTable.String(),
			Downloads: &models.DownloadList{
				CSV: &models.DownloadObject{
					Private: "s3://csv-exported/myfile.csv",
					HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.csv",
					Size:    "1234",
				},
			},
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		versionUpdate := &models.Version{
			State:        models.AssociatedState,
			ReleaseDate:  "2024-12-31",
			Version:      1,
			ID:           "789",
			CollectionID: "3434",
			Type:         models.CantabularFlexibleTable.String(),
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return errors.New("error while attempting to marshal generateDownloadsEvent to avro bytes")
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			UpdateDatasetWithAssociationFunc: func(context.Context, string, string, *models.Version) error {
				return nil
			},
		}

		states, transitions := setUpStatesTransitions()
		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := AssociateVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "error while attempting to marshal generateDownloadsEvent")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), convey.ShouldEqual, 1)
	})
}

func TestEditionConfirmVersionReturnsOK(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to edition-confirmed from completed", t, func() {
		currentVersion := &models.Version{
			State:        models.CompletedState,
			CollectionID: "3434",
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := EditionConfirmVersion(testContext, smDS, currentVersion, versionUpdateEditionConfirmed, versionDetails, "")

		convey.So(err, convey.ShouldEqual, nil)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
	})
}

func TestEditionConfirmVersionUpdateVersionFails(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to edition-confirmed from completed but the dataset is not found", t, func() {
		currentVersion := &models.Version{
			State:        models.CompletedState,
			CollectionID: "3434",
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", errs.ErrDatasetNotFound
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID: "789",
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
							ID:   "123",
						},
						Dimensions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017",
							ID:   "456",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
						},
					},
					ReleaseDate: "2017-12-12",
					State:       models.EditionConfirmedState,
					ETag:        "12345",
				}, nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := EditionConfirmVersion(testContext, smDS, currentVersion, versionUpdateEditionConfirmed, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "dataset not found")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 2)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
	})
}

func TestEditionConfirmVersionReturnsInvalidRequest(t *testing.T) {
	t.Parallel()
	convey.Convey("When the updated version is supplied without a state", t, func() {
		currentVersion := &models.Version{
			State:        models.CompletedState,
			CollectionID: "3434",
		}

		versionUpdate := &models.Version{
			ReleaseDate:  "2024-12-31",
			ID:           "789",
			CollectionID: "3434",
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := EditionConfirmVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "missing state")
	})
}

func TestPopulateVersonLinksIsNil(t *testing.T) {
	t.Parallel()
	convey.Convey("When the version links are nil and require populating", t, func() {
		versionLinks := models.VersionLinks{
			Spatial: &models.LinkObject{
				HRef: "http://this.is.alink",
			},
		}

		updatedLinks := populateVersionLinks(&versionLinks, nil)
		convey.So(updatedLinks.Spatial.HRef, convey.ShouldEqual, versionLinks.Spatial.HRef)
	})
}

func TestPopulateXLSDownloads(t *testing.T) {
	t.Parallel()
	convey.Convey("When the xls link requires populating", t, func() {
		currentVersionDownload := models.DownloadList{
			XLS: &models.DownloadObject{
				HRef: "http://links.to.download.xls",
			},
		}

		versionDownload := models.DownloadList{}

		updatedDownloads := populateDownloads(&versionDownload, &currentVersionDownload)
		convey.So(updatedDownloads.XLS, convey.ShouldEqual, currentVersionDownload.XLS)
	})
}

func TestPopulateVersionInfoFailsVersion(t *testing.T) {
	t.Parallel()
	convey.Convey("When the version number is invalid", t, func() {
		invalidVersionDetails := VersionDetails{
			datasetID: "123",
			version:   "bob",
			edition:   "2017",
		}

		mockedDataStore := &storetest.StorerMock{}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		currentVersion, combinedVersionUpdate, err := smDS.PopulateVersionInfo(testContext, publishVersionUpdate, invalidVersionDetails)

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "invalid version request")
		convey.So(currentVersion, convey.ShouldBeNil)
		convey.So(combinedVersionUpdate, convey.ShouldBeNil)
	})
}

func TestPopulateVersionInfoNilBody(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	convey.Convey("When the combined version can't be validated as the version update body is nil", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetDatasetFunc: func(_ context.Context, _ string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					ID: "123",
				}, nil
			},
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{}, nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		currentVersion, combinedVersionUpdate, err := smDS.PopulateVersionInfo(testContext, nil, versionDetails)

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "copy from must be non-nil and addressable")
		convey.So(currentVersion, convey.ShouldBeNil)
		convey.So(combinedVersionUpdate, convey.ShouldBeNil)
	})
}

func TestPopulateVersionInfoVersionNotFound(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	convey.Convey("When the version can't be found", t, func() {
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{}, errs.ErrVersionNotFound
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		currentVersion, combinedVersionUpdate, err := smDS.PopulateVersionInfo(testContext, publishVersionUpdate, versionDetails)

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "version not found")
		convey.So(currentVersion, convey.ShouldBeNil)
		convey.So(combinedVersionUpdate, convey.ShouldBeNil)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
	})
}

func TestPopulateVersionInfoErrors(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	convey.Convey("When the edition can't be found", t, func() {
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		currentVersion, combinedVersionUpdate, err := smDS.PopulateVersionInfo(testContext, publishVersionUpdate, versionDetails)

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "edition not found")
		convey.So(currentVersion, convey.ShouldBeNil)
		convey.So(combinedVersionUpdate, convey.ShouldBeNil)
		convey.So(len(mockedDataStore.CheckEditionExistsCalls()), convey.ShouldEqual, 1)
	})
}

func TestPopulateNewVersionDocWithDownloads(t *testing.T) {
	t.Parallel()
	convey.Convey("Valid versions provided with downloads", t, func() {
		currentVersion := &models.Version{
			State:         models.PublishedState,
			ReleaseDate:   "2024-12-31",
			Version:       1,
			ID:            "789",
			LatestChanges: &[]models.LatestChange{{Description: "this is a change"}},
			Links:         &models.VersionLinks{Spatial: &models.LinkObject{HRef: "http://this.is.a.spatial.link"}},
			Downloads:     &models.DownloadList{XLS: &models.DownloadObject{HRef: "http://link.to.xls.file"}},
		}

		originalVersion := &models.Version{
			ID:            "789",
			Alerts:        &[]models.Alert{{Description: "this is an alert"}},
			LatestChanges: &[]models.LatestChange{{Description: "this is also a change"}},
			Links:         &models.VersionLinks{Spatial: &models.LinkObject{HRef: "http://this.is.a.spatial.link"}},
			Downloads:     &models.DownloadList{XLS: &models.DownloadObject{HRef: "http://link.to.xls.file"}},
		}

		version, err := populateNewVersionDoc(currentVersion, originalVersion)
		convey.So(err, convey.ShouldBeNil)
		convey.So(version, convey.ShouldNotBeNil)
		convey.So(version.ReleaseDate, convey.ShouldEqual, currentVersion.ReleaseDate)
		convey.So(version.State, convey.ShouldEqual, currentVersion.State)
		convey.So(version.Downloads.XLS, convey.ShouldEqual, currentVersion.Downloads.XLS)
		convey.So(len(*version.LatestChanges), convey.ShouldEqual, len(*currentVersion.LatestChanges)+len(*originalVersion.LatestChanges))
	})
}

func TestPublishVersionForInvalidTypeNoDownloader(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to published from associated for a type that does not generate downloads", t, func() {
		currentVersion := &models.Version{
			State: models.AssociatedState,
			Type:  "static",
			Downloads: &models.DownloadList{
				CSV: &models.DownloadObject{
					Private: "s3://csv-exported/myfile.csv",
					HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.csv",
					Size:    "1234",
				},
			},
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		versionUpdate := &models.Version{
			State:       models.PublishedState,
			Version:     1,
			ReleaseDate: "2024-05-23",
			ID:          "789",
			Type:        "static",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
				return nil
			},
			GetDatasetFunc: func(context.Context, string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					Next: &models.Dataset{Links: &models.DatasetLinks{LatestVersion: &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
						ID: "1"}}},
					ID: "123",
				}, nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "123",
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
					Current: &models.Edition{
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
				}, nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		states, transitions := setUpStatesTransitions()
		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "no downloader available for type")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
	})
}

func TestPublishCMDVersionFailsToPublish(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to published from associated and the graph errors", t, func() {
		currentVersion := &models.Version{
			State:        models.AssociatedState,
			CollectionID: "3434",
			Type:         models.Filterable.String(),
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		versionUpdate := &models.Version{
			State:       models.PublishedState,
			ReleaseDate: "2024-12-31",
			Version:     1,
			ID:          "789",
			Type:        models.Filterable.String(),
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "123",
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
					Current: &models.Edition{
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
				}, nil
			},
			UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
				return nil
			},
			SetInstanceIsPublishedFunc: func(context.Context, string) error {
				return errors.New("failed to set is_published on the instance node")
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "failed to set is_published on the instance node")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 1)
	})
}

func TestPublishVersionDatabaseFails(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to published from associated and the dataset is not found", t, func() {
		currentVersion := &models.Version{
			State:        models.AssociatedState,
			CollectionID: "3434",
			Type:         models.CantabularFlexibleTable.String(),
			Links: &models.VersionLinks{

				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		versionUpdate := &models.Version{
			State:       models.PublishedState,
			ReleaseDate: "2024-12-31",
			Version:     1,
			ID:          "789",
			Type:        models.CantabularFlexibleTable.String(),
			Links: &models.VersionLinks{

				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			GetDatasetFunc: func(_ context.Context, _ string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					Next: &models.Dataset{Links: &models.DatasetLinks{LatestVersion: &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
						ID: "1"}}},
					ID: "123",
				}, nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "123",
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
					Current: &models.Edition{
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
				}, nil
			},
			UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
				return nil
			},
			SetInstanceIsPublishedFunc: func(context.Context, string) error {
				return nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return errs.ErrDatasetNotFound
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "dataset not found")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)
	})
}

func TestPublishVersionDatasetNotFound(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to published from associated and the dataset is not found", t, func() {
		currentVersion := &models.Version{
			State:        models.AssociatedState,
			CollectionID: "3434",
			Type:         models.CantabularFlexibleTable.String(),
			Links: &models.VersionLinks{

				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		versionUpdate := &models.Version{
			State:       models.PublishedState,
			ReleaseDate: "2024-12-31",
			Version:     1,
			ID:          "789",
			Type:        models.CantabularFlexibleTable.String(),
			Links: &models.VersionLinks{

				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			GetDatasetFunc: func(_ context.Context, _ string) (*models.DatasetUpdate, error) {
				return nil, errs.ErrDatasetNotFound
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "123",
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
					Current: &models.Edition{
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
				}, nil
			},
			UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
				return nil
			},
			SetInstanceIsPublishedFunc: func(context.Context, string) error {
				return nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "dataset not found")
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 1)
	})
}

func TestPublishVersionInvalidType(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to published from associated with an incorrect type", t, func() {
		currentVersion := &models.Version{
			State:        models.AssociatedState,
			CollectionID: "3434",
			Type:         "not_a_type",
			Downloads: &models.DownloadList{
				CSV: &models.DownloadObject{
					Private: "s3://csv-exported/myfile.csv",
					Public:  "http://the.public.link.csv",
					HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.csv",
					Size:    "1234",
				},
				XLS: &models.DownloadObject{
					Private: "s3://csv-exported/myfile.xls",
					HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.xls",
					Size:    "1234",
				},
			},
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		versionUpdate := &models.Version{
			State:       models.PublishedState,
			ReleaseDate: "2024-12-31",
			Version:     1,
			ID:          "789",
			Type:        "not_a_type",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "123",
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
					Current: &models.Edition{
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
				}, nil
			},
			UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
				return nil
			},
		}

		states, transitions := setUpStatesTransitions()
		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "invalid dataset type")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
	})
}

func TestPublishVersionDatasetDownloadsOK(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to published from associated", t, func() {
		currentVersion := &models.Version{
			State:        models.AssociatedState,
			CollectionID: "3434",
			Type:         models.CantabularFlexibleTable.String(),
			Downloads: &models.DownloadList{
				CSV: &models.DownloadObject{
					Private: "s3://csv-exported/myfile.csv",
					HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.csv",
					Size:    "1234",
				},
				XLS: &models.DownloadObject{
					Private: "s3://csv-exported/myfile.xls",
					HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.xls",
					Size:    "1234",
				},
			},
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		versionUpdate := &models.Version{
			State:       models.PublishedState,
			ReleaseDate: "2024-12-31",
			Version:     1,
			ID:          "789",
			Type:        models.CantabularFlexibleTable.String(),
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			GetDatasetFunc: func(_ context.Context, _ string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					Next: &models.Dataset{Links: &models.DatasetLinks{LatestVersion: &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
						ID: "1"}}},
					ID: "123",
				}, nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "123",
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
					Current: &models.Edition{
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
				}, nil
			},
			UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
				return nil
			},
			SetInstanceIsPublishedFunc: func(context.Context, string) error {
				return nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldBeNil)
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)
	})
}

func TestPublishVersionFailedToUpdate(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to published from associated but the dataset is not found", t, func() {
		currentVersion := &models.Version{
			State: models.AssociatedState,
			Type:  models.CantabularFlexibleTable.String(),
			Downloads: &models.DownloadList{
				CSV: &models.DownloadObject{
					Private: "s3://csv-exported/myfile.csv",
					HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.csv",
					Size:    "1234",
				},
			},
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		versionUpdate := &models.Version{
			State:       models.PublishedState,
			ReleaseDate: "2024-12-31",
			Version:     1,
			ID:          "789",
			Type:        models.CantabularFlexibleTable.String(),
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", errs.ErrDatasetNotFound
			},
			GetVersionFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return nil, errs.ErrVersionNotFound
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "version not found")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetVersionCalls()), convey.ShouldEqual, 1)
	})
}

func TestPublishVersionPublishLinksFails(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to published from associated, but amending the links fails", t, func() {
		currentVersion := &models.Version{
			State: models.AssociatedState,
			Type:  models.CantabularFlexibleTable.String(),
			Downloads: &models.DownloadList{
				CSV: &models.DownloadObject{
					Private: "s3://csv-exported/myfile.csv",
					HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.csv",
					Size:    "1234",
				},
			},
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
			},
		}

		versionUpdate := &models.Version{
			State:       models.PublishedState,
			ReleaseDate: "2024-12-31",
			Version:     1,
			ID:          "789",
			Type:        models.CantabularFlexibleTable.String(),
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
			},
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "123",
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
					Current: &models.Edition{
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
				}, nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "invalid arguments to PublishLinks - versionLink empty")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
	})
}

func TestPublishVersionUpsertEditionFails(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to published from associated but the edition is not found", t, func() {
		currentVersion := &models.Version{
			State: models.AssociatedState,
			Type:  models.CantabularFlexibleTable.String(),
			Downloads: &models.DownloadList{
				CSV: &models.DownloadObject{
					Private: "s3://csv-exported/myfile.csv",
					HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.csv",
					Size:    "1234",
				},
			},
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		versionUpdate := &models.Version{
			State:       models.PublishedState,
			ReleaseDate: "2024-12-31",
			Version:     1,
			ID:          "789",
			Type:        models.CantabularFlexibleTable.String(),
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "123",
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
					Current: &models.Edition{
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
				}, nil
			},
			UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
				return errs.ErrEditionNotFound
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "edition not found")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
	})
}

func TestPublishVersionFailedToGenerateDownloads(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to published from associated but the downloads fail to generate", t, func() {
		currentVersion := &models.Version{
			State:        models.AssociatedState,
			CollectionID: "3434",
			Type:         models.CantabularFlexibleTable.String(),
			Downloads: &models.DownloadList{
				CSV: &models.DownloadObject{
					Private: "s3://csv-exported/myfile.csv",
					HRef:    "http://localhost:23600/datasets/123/editions/2017/versions/1.csv",
					Size:    "1234",
				},
			},
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		versionUpdate := &models.Version{
			State:       models.PublishedState,
			ReleaseDate: "2024-12-31",
			Version:     1,
			ID:          "789",
			Type:        models.CantabularFlexibleTable.String(),
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return errors.New("error while attempting to marshal generateDownloadsEvent to avro bytes")
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			GetDatasetFunc: func(ctx context.Context, datasetID string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					Next: &models.Dataset{Links: &models.DatasetLinks{LatestVersion: &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
						ID: "1"}}},
					ID: "123",
				}, nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "123",
					Next: &models.Edition{
						State: models.EditionConfirmedState,
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
					Current: &models.Edition{
						Links: &models.EditionUpdateLinks{
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
								ID:   "1",
							},
						},
					},
				}, nil
			},
			UpsertEditionFunc: func(context.Context, string, string, *models.EditionUpdate) error {
				return nil
			},
			SetInstanceIsPublishedFunc: func(context.Context, string) error {
				return nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "error while attempting to marshal generateDownloadsEvent")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetDatasetCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertEditionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.SetInstanceIsPublishedCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.UpsertDatasetCalls()), convey.ShouldEqual, 1)
	})
}

func TestPublishVersionFailedToFindEdition(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to published from associated", t, func() {
		currentVersion := &models.Version{
			State:        models.AssociatedState,
			CollectionID: "3434",
			Type:         models.CantabularFlexibleTable.String(),
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		versionUpdate := &models.Version{
			State:       models.PublishedState,
			ReleaseDate: "2024-12-31",
			Version:     1,
			ID:          "789",
			Type:        models.CantabularFlexibleTable.String(),
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017",
					ID:   "2017",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/765",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
					ID:   "1",
				},
			},
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{
			UpdateVersionFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			GetEditionFunc: func(context.Context, string, string, string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "edition not found")
		convey.So(len(mockedDataStore.UpdateVersionCalls()), convey.ShouldEqual, 1)
		convey.So(len(mockedDataStore.GetEditionCalls()), convey.ShouldEqual, 1)
	})
}

func TestPublishMissingRequiredField(t *testing.T) {
	t.Parallel()
	convey.Convey("When a version is set to published from associated and the release date is missing", t, func() {
		currentVersion := &models.Version{
			State:        models.AssociatedState,
			CollectionID: "3434",
			Type:         models.CantabularFlexibleTable.String(),
		}

		invalidVersionUpdate := &models.Version{
			State:   models.PublishedState,
			Version: 1,
			Type:    models.CantabularFlexibleTable.String(),
		}

		generatorMock := &mocks.DownloadsGeneratorMock{
			GenerateFunc: func(context.Context, string, string, string, string) error {
				return nil
			},
		}

		mockedDataStore := &storetest.StorerMock{}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, invalidVersionUpdate, versionDetails, trueStringified)

		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "missing mandatory fields: [release_date]")
	})
}

func GetStateMachineAPIWithCMDMocks(mockedDataStore store.Storer, mockedGeneratedDownloads DownloadsGenerator, statemachine *StateMachine) *StateMachineDatasetAPI {
	mockedMapSMGeneratedDownloads := map[models.DatasetType]DownloadsGenerator{
		models.Filterable:              mockedGeneratedDownloads,
		models.CantabularBlob:          mockedGeneratedDownloads,
		models.CantabularTable:         mockedGeneratedDownloads,
		models.CantabularFlexibleTable: mockedGeneratedDownloads,
	}

	return Setup(store.DataStore{Backend: mockedDataStore}, mockedMapSMGeneratedDownloads, statemachine)
}
