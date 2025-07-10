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
	. "github.com/smartystreets/goconvey/convey"
)

var testContext = context.Background()

var publishVersionUpdate = &models.Version{
	State:        models.PublishedState,
	ReleaseDate:  "2024-12-31",
	ID:           "a1b2c3",
	CollectionID: "3434",
}

var publishVersionUpdateStatic = &models.Version{
	State:        models.PublishedState,
	ReleaseDate:  "2024-12-31",
	ID:           "a1b2c3",
	CollectionID: "3434",
	Type:         "static",
}

var currentVersionEditionConfirmed = &models.Version{
	State:        models.EditionConfirmedState,
	CollectionID: "3434",
	Type:         "v4",
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
	Type:         "cantabular_flexible_table",
}

var versionUpdateAssociatedStatic = &models.Version{
	State:        models.AssociatedState,
	ReleaseDate:  "2024-12-31",
	Version:      1,
	ID:           "789",
	CollectionID: "3434",
	Type:         "static",
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
		Label:               "published",
		TargetState:         Published,
		AllowedSourceStates: []string{"associated", "published", "edition-confirmed"},
		Type:                "v4",
	}, {
		Label:               "associated",
		TargetState:         Associated,
		AllowedSourceStates: []string{"edition-confirmed", "associated"},
		Type:                "v4",
	}, {
		Label:               "edition-confirmed",
		TargetState:         EditionConfirmed,
		AllowedSourceStates: []string{"edition-confirmed", "completed", "published"},
		Type:                "v4",
	},
		{
			Label:               "published",
			TargetState:         Published,
			AllowedSourceStates: []string{"associated", "published", "edition-confirmed"},
			Type:                "cantabular_flexible_table",
		}, {
			Label:               "associated",
			TargetState:         Associated,
			AllowedSourceStates: []string{"edition-confirmed", "associated", "created"},
			Type:                "cantabular_flexible_table",
		}, {
			Label:               "edition-confirmed",
			TargetState:         EditionConfirmed,
			AllowedSourceStates: []string{"edition-confirmed", "completed", "published"},
			Type:                "cantabular_flexible_table",
		},
		{
			Label:               "published",
			TargetState:         Published,
			AllowedSourceStates: []string{"associated", "published", "edition-confirmed"},
			Type:                "static",
		}, {
			Label:               "associated",
			TargetState:         Associated,
			AllowedSourceStates: []string{"edition-confirmed", "associated", "created"},
			Type:                "static",
		}, {
			Label:               "edition-confirmed",
			TargetState:         EditionConfirmed,
			AllowedSourceStates: []string{"edition-confirmed", "completed", "published"},
			Type:                "static",
		},
	}

	return states, transitions
}

func TestAmendVersionInvalidState(t *testing.T) {
	t.Parallel()
	Convey("When a version tries to transition from created to published", t, func() {
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

		amendedVersion, err := smDS.AmendVersion(testContext, vars, publishVersionUpdate)
		So(err, ShouldNotBeNil)
		So(amendedVersion, ShouldBeNil)
		So(err.Error(), ShouldContainSubstring, "state not allowed to transition")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.AcquireInstanceLockCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UnlockInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAmendVersionPopulateModelsFails(t *testing.T) {
	t.Parallel()
	Convey("When a version tries to transition from associated to published and the edition is not found", t, func() {
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

		amendedVersion, err := smDS.AmendVersion(testContext, vars, publishVersionUpdate)
		So(err, ShouldNotBeNil)
		So(amendedVersion, ShouldBeNil)
		So(err.Error(), ShouldContainSubstring, "edition not found")
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.AcquireInstanceLockCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UnlockInstanceCalls()), ShouldEqual, 1)
	})
}

func TestAmendVersionPopulateVersionFails(t *testing.T) {
	t.Parallel()
	Convey("When a version tries to transition from edition-confirmed to associated and the model is not valid", t, func() {
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

		amendedVersion, err := smDS.AmendVersion(testContext, vars, versionUpdateInvalid)
		So(err, ShouldNotBeNil)
		So(amendedVersion, ShouldBeNil)
		So(err.Error(), ShouldContainSubstring, "missing mandatory fields: [release_date]")
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.AcquireInstanceLockCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UnlockInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestAmendVersionStaticSuccess(t *testing.T) {
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

	Convey("When a request is made to change a version from associated to published but the dataset is not found", t, func() {
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			UpdateVersionStaticFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			UpdateDatasetWithAssociationFunc: func(context.Context, string, string, *models.Version) error {
				return nil
			},
			GetVersionStaticFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
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
					Type:        "static",
				}, nil
			},
			AcquireVersionsLockFunc: func(context.Context, string) (string, error) {
				return "", nil
			},
			UnlockVersionsFunc: func(context.Context, string) {},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})
		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		amendedVersion, err := smDS.AmendVersion(testContext, vars, versionUpdateAssociatedStatic)
		So(err, ShouldBeNil)
		So(amendedVersion, ShouldNotBeNil)
		So(amendedVersion.State, ShouldEqual, models.AssociatedState)
		So(len(mockedDataStore.AcquireVersionsLockCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UnlockVersionsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionStaticCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionStaticCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 0)
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

	Convey("When a request is made to change a version from associated to published but the dataset is not found", t, func() {
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
					Type:        "cantabular_flexible_table",
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

		amendedVersion, err := smDS.AmendVersion(testContext, vars, versionUpdateAssociated)
		So(err, ShouldBeNil)
		So(amendedVersion, ShouldNotBeNil)
		So(amendedVersion.State, ShouldEqual, models.AssociatedState)
		So(len(mockedDataStore.AcquireInstanceLockCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UnlockInstanceCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
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

	Convey("When a request is made to change state from associated to published but the instances collection lock fails", t, func() {
		mockedDataStore := &storetest.StorerMock{
			AcquireInstanceLockFunc: func(context.Context, string) (string, error) {
				return "", errors.New("Unable to acquire lock")
			},
			UnlockInstanceFunc: func(context.Context, string) {},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})
		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		amendedVersion, err := smDS.AmendVersion(testContext, vars, publishVersionUpdate)

		So(err, ShouldNotBeNil)
		So(amendedVersion, ShouldBeNil)
		So(err.Error(), ShouldContainSubstring, "Unable to acquire lock")
	})

	Convey("When a request is made to change state from associated to published but the versions collection lock fails", t, func() {
		mockedDataStore := &storetest.StorerMock{
			AcquireVersionsLockFunc: func(context.Context, string) (string, error) {
				return "", errors.New("Unable to acquire lock")
			},
			UnlockVersionsFunc: func(context.Context, string) {},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})
		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		amendedVersion, err := smDS.AmendVersion(testContext, vars, publishVersionUpdateStatic)

		So(err, ShouldNotBeNil)
		So(amendedVersion, ShouldBeNil)
		So(err.Error(), ShouldContainSubstring, "Unable to acquire lock")
	})
}

func TestAssociateVersionInvalidVersion(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	Convey("When a version is set to associated from edition-confirmed and the version is invalid", t, func() {
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "invalid version requested")
	})
}

func TestAssociateVersionInvalidType(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	Convey("When a version is set to published from associated with an incorrect type", t, func() {
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "error getting type of version: invalid dataset type")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
	})
}

func TestAssociateVersionInvalidRequest(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	Convey("When a version is set to associated from edition-confirmed and is missing a release date", t, func() {
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "missing mandatory fields: [release_date]")
	})
}

func TestAssociateStaticVersionNoErrors(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	Convey("When a version is set to associated from edition-confirmed for a type which does not generate downloads", t, func() {
		currentStaticVersion := &models.Version{
			State:        models.CreatedState,
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
			UpdateVersionStaticFunc: func(ctx context.Context, currentVersion *models.Version, versionUpdate *models.Version, eTagSelector string) (string, error) {
				return "", nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := AssociateVersion(testContext, smDS, currentStaticVersion, versionstaticUpdate, versionDetails, "")

		So(err, ShouldBeNil)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 0)
		So(len(mockedDataStore.UpdateVersionStaticCalls()), ShouldEqual, 1)
	})
}

func TestAssociateVersionErrors(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	Convey("When a version is set to associated from edition-confirmed but the dataset is not found", t, func() {
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "dataset not found")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
	})
}

func TestAssociateVersionFailedToGenerateDownloads(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to published from associated and the downloads fail to generate", t, func() {
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "error while attempting to marshal generateDownloadsEvent")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateDatasetWithAssociationCalls()), ShouldEqual, 1)
	})
}

func TestApproveVersionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to approved from associated", t, func() {
		currentVersion := &models.Version{
			State:        models.ApprovedState,
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

		So(err, ShouldEqual, nil)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
	})
}

func TestApproveVersionFails(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to approved from associated but the dataset is not found", t, func() {
		currentVersion := &models.Version{
			State:        models.AssociatedState,
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "dataset not found")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestApproveVersionReturnsInvalidRequest(t *testing.T) {
	t.Parallel()
	Convey("When the updated version is supplied without a state", t, func() {
		currentVersion := &models.Version{
			State:        models.AssociatedState,
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "missing state")
	})
}

func TestEditionConfirmVersionReturnsOK(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to edition-confirmed from completed", t, func() {
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

		So(err, ShouldEqual, nil)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
	})
}

func TestEditionConfirmVersionUpdateVersionFails(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to edition-confirmed from completed but the dataset is not found", t, func() {
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "dataset not found")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 2)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestEditionConfirmVersionReturnsInvalidRequest(t *testing.T) {
	t.Parallel()
	Convey("When the updated version is supplied without a state", t, func() {
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "missing state")
	})
}

func TestPopulateVersonLinksIsNil(t *testing.T) {
	t.Parallel()
	Convey("When the version links are nil and require populating", t, func() {
		versionLinks := models.VersionLinks{
			Spatial: &models.LinkObject{
				HRef: "http://this.is.alink",
			},
		}

		updatedLinks := populateVersionLinks(&versionLinks, nil)
		So(updatedLinks.Spatial.HRef, ShouldEqual, versionLinks.Spatial.HRef)
	})
}

func TestPopulateXLSDownloads(t *testing.T) {
	t.Parallel()
	Convey("When the xls link requires populating", t, func() {
		currentVersionDownload := models.DownloadList{
			XLS: &models.DownloadObject{
				HRef: "http://links.to.download.xls",
			},
		}

		versionDownload := models.DownloadList{}

		updatedDownloads := populateDownloads(&versionDownload, &currentVersionDownload)
		So(updatedDownloads.XLS, ShouldEqual, currentVersionDownload.XLS)
	})
}

func TestPopulateVersionInfoFailsVersion(t *testing.T) {
	t.Parallel()
	Convey("When the version number is invalid", t, func() {
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "invalid version request")
		So(currentVersion, ShouldBeNil)
		So(combinedVersionUpdate, ShouldBeNil)
	})
}

func TestPopulateVersionInfoNilBody(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	Convey("When the combined version can't be validated as the version update body is nil", t, func() {
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "copy from must be non-nil and addressable")
		So(currentVersion, ShouldBeNil)
		So(combinedVersionUpdate, ShouldBeNil)
	})
}

func TestPopulateVersionInfoVersionNotFound(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	Convey("When the version can't be found", t, func() {
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "version not found")
		So(currentVersion, ShouldBeNil)
		So(combinedVersionUpdate, ShouldBeNil)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})

	Convey("When the static version can't be found", t, func() {
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return nil
			},
			GetVersionStaticFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{}, errs.ErrVersionNotFound
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		currentVersion, combinedVersionUpdate, err := smDS.PopulateVersionInfo(testContext, publishVersionUpdateStatic, versionDetails)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "version not found")
		So(currentVersion, ShouldBeNil)
		So(combinedVersionUpdate, ShouldBeNil)
		So(len(mockedDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionStaticCalls()), ShouldEqual, 1)
	})
}

func TestPopulateVersionInfoErrors(t *testing.T) {
	t.Parallel()

	generatorMock := &mocks.DownloadsGeneratorMock{
		GenerateFunc: func(context.Context, string, string, string, string) error {
			return nil
		},
	}

	Convey("When the edition can't be found", t, func() {
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsFunc: func(context.Context, string, string, string) error {
				return errs.ErrEditionNotFound
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		currentVersion, combinedVersionUpdate, err := smDS.PopulateVersionInfo(testContext, publishVersionUpdate, versionDetails)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "edition not found")
		So(currentVersion, ShouldBeNil)
		So(combinedVersionUpdate, ShouldBeNil)
		So(len(mockedDataStore.CheckEditionExistsCalls()), ShouldEqual, 1)
	})

	Convey("When the version can't be found", t, func() {
		mockedDataStore := &storetest.StorerMock{
			CheckEditionExistsStaticFunc: func(context.Context, string, string, string) error {
				return errs.ErrVersionNotFound
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)

		currentVersion, combinedVersionUpdate, err := smDS.PopulateVersionInfo(testContext, publishVersionUpdateStatic, versionDetails)

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "version not found")
		So(currentVersion, ShouldBeNil)
		So(combinedVersionUpdate, ShouldBeNil)
		So(len(mockedDataStore.CheckEditionExistsStaticCalls()), ShouldEqual, 1)
	})
}

func TestPopulateNewVersionDocWithDownloads(t *testing.T) {
	t.Parallel()
	Convey("Valid versions provided with downloads", t, func() {
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
		So(err, ShouldBeNil)
		So(version, ShouldNotBeNil)
		So(version.ReleaseDate, ShouldEqual, currentVersion.ReleaseDate)
		So(version.State, ShouldEqual, currentVersion.State)
		So(version.Downloads.XLS, ShouldEqual, currentVersion.Downloads.XLS)
		So(len(*version.LatestChanges), ShouldEqual, len(*currentVersion.LatestChanges)+len(*originalVersion.LatestChanges))
	})
}

func TestPopulateNewVersionDocWithDistributions(t *testing.T) {
	t.Parallel()
	Convey("Given versions with distributions", t, func() {
		currentVersion := &models.Version{
			Distributions: &[]models.Distribution{
				{
					Title:       "Distribution 1",
					Format:      "csv",
					MediaType:   "text/csv",
					DownloadURL: "/link/to/distribution1.csv",
					ByteSize:    1234,
				},
				{
					Title:       "Distribution 2",
					Format:      "csv",
					MediaType:   "text/csv",
					DownloadURL: "/link/to/distribution2.csv",
					ByteSize:    5678,
				},
			},
		}

		originalVersion := &models.Version{
			Distributions: &[]models.Distribution{
				{
					Title:       "Distribution 3",
					Format:      "csv",
					MediaType:   "text/csv",
					DownloadURL: "/link/to/distribution3.csv",
					ByteSize:    4321,
				},
				{
					Title:       "Distribution 4",
					Format:      "csv",
					MediaType:   "text/csv",
					DownloadURL: "/link/to/distribution4.csv",
					ByteSize:    8765,
				},
			},
		}

		Convey("When the version type is static", func() {
			currentVersion.Type = models.Static.String()
			originalVersion.Type = models.Static.String()

			Convey("Then the distributions are set correctly", func() {
				version, err := populateNewVersionDoc(currentVersion, originalVersion)
				So(err, ShouldBeNil)
				So(version, ShouldNotBeNil)
				So(version.Type, ShouldEqual, models.Static.String())
				So(len(*version.Distributions), ShouldEqual, 2)
				So((*version.Distributions)[0].Title, ShouldEqual, "Distribution 3")
				So((*version.Distributions)[0].Format.String(), ShouldEqual, "csv")
				So((*version.Distributions)[0].MediaType.String(), ShouldEqual, "text/csv")
				So((*version.Distributions)[0].DownloadURL, ShouldEqual, "/link/to/distribution3.csv")
				So((*version.Distributions)[0].ByteSize, ShouldEqual, 4321)

				So((*version.Distributions)[1].Title, ShouldEqual, "Distribution 4")
				So((*version.Distributions)[1].Format.String(), ShouldEqual, "csv")
				So((*version.Distributions)[1].MediaType.String(), ShouldEqual, "text/csv")
				So((*version.Distributions)[1].DownloadURL, ShouldEqual, "/link/to/distribution4.csv")
				So((*version.Distributions)[1].ByteSize, ShouldEqual, 8765)
			})
		})

		Convey("When the version type is static and the version update has no distributions", func() {
			currentVersion.Type = models.Static.String()
			originalVersion.Type = models.Static.String()
			originalVersion.Distributions = nil

			Convey("Then the distributions are set to what the currentVersion distributions contained", func() {
				version, err := populateNewVersionDoc(currentVersion, originalVersion)
				So(err, ShouldBeNil)
				So(version, ShouldNotBeNil)
				So(version.Type, ShouldEqual, models.Static.String())
				So(len(*version.Distributions), ShouldEqual, 2)
				So((*version.Distributions)[0].Title, ShouldEqual, "Distribution 1")
				So((*version.Distributions)[0].Format.String(), ShouldEqual, "csv")
				So((*version.Distributions)[0].MediaType.String(), ShouldEqual, "text/csv")
				So((*version.Distributions)[0].DownloadURL, ShouldEqual, "/link/to/distribution1.csv")
				So((*version.Distributions)[0].ByteSize, ShouldEqual, 1234)

				So((*version.Distributions)[1].Title, ShouldEqual, "Distribution 2")
				So((*version.Distributions)[1].Format.String(), ShouldEqual, "csv")
				So((*version.Distributions)[1].MediaType.String(), ShouldEqual, "text/csv")
				So((*version.Distributions)[1].DownloadURL, ShouldEqual, "/link/to/distribution2.csv")
				So((*version.Distributions)[1].ByteSize, ShouldEqual, 5678)
			})
		})

		Convey("When the version type is not static", func() {
			currentVersion.Type = models.Filterable.String()
			originalVersion.Type = models.Filterable.String()

			Convey("Then the distributions are not set", func() {
				version, err := populateNewVersionDoc(currentVersion, originalVersion)
				So(err, ShouldBeNil)
				So(version, ShouldNotBeNil)
				So(version.Type, ShouldEqual, models.Filterable.String())
				So(version.Distributions, ShouldBeNil)
			})
		})
	})
}

func TestPublishCMDVersionFailsToPublish(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to published from associated and the graph errors", t, func() {
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
			GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return models.Filterable.String(), nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "failed to set is_published on the instance node")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 1)
	})
}

func TestPublishVersionDatabaseFails(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to published from associated and the dataset is not found", t, func() {
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
			GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "dataset not found")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
	})
}

func TestPublishVersionDatasetNotFound(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to published from associated and the dataset is not found", t, func() {
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
			GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "dataset not found")
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 1)
	})
}

func TestPublishVersionInvalidType(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to published from associated with an incorrect type", t, func() {
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
			GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return "not_a_type", nil
			},
		}

		states, transitions := setUpStatesTransitions()
		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "invalid dataset type")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
	})
}

func TestPublishVersionDatasetDownloadsOK(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to published from associated", t, func() {
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
			GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		So(err, ShouldBeNil)
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
	})

	Convey("When a static version is set to published from associated", t, func() {
		currentVersion := &models.Version{
			State:        models.AssociatedState,
			CollectionID: "3434",
			Type:         models.Static.String(),
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
			Type:        models.Static.String(),
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
			UpdateVersionStaticFunc: func(context.Context, *models.Version, *models.Version, string) (string, error) {
				return "", nil
			},
			GetDatasetFunc: func(_ context.Context, _ string) (*models.DatasetUpdate, error) {
				return &models.DatasetUpdate{
					Next: &models.Dataset{Links: &models.DatasetLinks{LatestVersion: &models.LinkObject{HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
						ID: "1"}}},
					ID: "123",
				}, nil
			},
			GetVersionStaticFunc: func(context.Context, string, string, int, string) (*models.Version, error) {
				return &models.Version{
					ID:    "123",
					State: models.EditionConfirmedState,
					Links: &models.VersionLinks{
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017",
						},
						Version: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/2017/versions/1",
							ID:   "1",
						},
					},
				}, nil
			},
			UpsertVersionStaticFunc: func(context.Context, string, *models.Version) error {
				return nil
			},
			UpsertDatasetFunc: func(context.Context, string, *models.DatasetUpdate) error {
				return nil
			},
			GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return models.Static.String(), nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		So(err, ShouldBeNil)
		So(len(mockedDataStore.UpdateVersionStaticCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionStaticCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertVersionStaticCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetTypeCalls()), ShouldEqual, 1)
	})
}

func TestPublishVersionFailedToUpdate(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to published from associated but the dataset is not found", t, func() {
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "version not found")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetVersionCalls()), ShouldEqual, 1)
	})
}

func TestPublishVersionPublishLinksFails(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to published from associated, but amending the links fails", t, func() {
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
			GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "invalid arguments to PublishLinks - versionLink empty")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})
}

func TestPublishVersionUpsertEditionFails(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to published from associated but the edition is not found", t, func() {
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
			GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "edition not found")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
	})
}

func TestPublishVersionFailedToGenerateDownloads(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to published from associated but the downloads fail to generate", t, func() {
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
			GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "error while attempting to marshal generateDownloadsEvent")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetDatasetCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertEditionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.SetInstanceIsPublishedCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.UpsertDatasetCalls()), ShouldEqual, 1)
	})
}

func TestPublishVersionFailedToFindEdition(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to published from associated", t, func() {
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
			GetDatasetTypeFunc: func(ctx context.Context, datasetID string, authorised bool) (string, error) {
				return models.CantabularFlexibleTable.String(), nil
			},
		}

		states, transitions := setUpStatesTransitions()

		stateMachine := NewStateMachine(testContext, states, transitions, store.DataStore{Backend: mockedDataStore})

		smDS := GetStateMachineAPIWithCMDMocks(mockedDataStore, generatorMock, stateMachine)
		err := PublishVersion(testContext, smDS, currentVersion, versionUpdate, versionDetails, "")

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "edition not found")
		So(len(mockedDataStore.UpdateVersionCalls()), ShouldEqual, 1)
		So(len(mockedDataStore.GetEditionCalls()), ShouldEqual, 1)
	})
}

func TestPublishMissingRequiredField(t *testing.T) {
	t.Parallel()
	Convey("When a version is set to published from associated and the release date is missing", t, func() {
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

		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "missing mandatory fields: [release_date]")
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
