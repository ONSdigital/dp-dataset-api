package instance

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"context"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	testDatasetID   = "1234"
	testEditionName = "test-edition"
	testHost        = "example.com"
	testInstanceID  = "new-instance-1234"
)

func Test_ConfirmEditionReturnsOK(t *testing.T) {
	Convey("given no edition exists", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(ctx context.Context, dataset, edition, state string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
			UpsertEditionFunc: func(ctx context.Context, dataset, edition string, doc *models.EditionUpdate) error {
				return nil
			},
		}

		s := Store{
			Storer: mockedDataStore,
			Host:   testHost,
		}

		Convey("when confirmEdition is called", func() {
			edition, err := s.confirmEdition(ctx, testDatasetID, testEditionName, testInstanceID)

			Convey("then an edition is created and the version ID is 1", func() {
				So(edition, ShouldNotBeNil)
				So(err, ShouldBeNil)

				So(len(edition.ID), ShouldBeGreaterThan, 0)
				So(edition.Current, ShouldBeNil)
				So(edition.Next, ShouldNotBeNil)

				So(edition.Next, ShouldResemble, &models.Edition{
					Edition: testEditionName,
					State:   models.EditionConfirmedState,
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							ID:   testDatasetID,
							HRef: fmt.Sprintf("%s/datasets/%s", s.Host, testDatasetID),
						},
						Self: &models.LinkObject{
							HRef: fmt.Sprintf("%s/datasets/%s/editions/%s", s.Host, testDatasetID, testEditionName),
						},
						Versions: &models.LinkObject{
							HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions", s.Host, testDatasetID, testEditionName),
						},
						LatestVersion: &models.LinkObject{
							ID:   "1",
							HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions/1", s.Host, testDatasetID, testEditionName),
						},
					},
				})
			})
		})
	})

	// TODO conditional test for feature flagged functionality. Will need tidying up eventually.
	featureEnvString := os.Getenv("ENABLE_DETACH_DATASET")
	featureOn, _ := strconv.ParseBool(featureEnvString)
	if featureOn {
		Convey("given an edition exists with 1 unpublished version", t, func() {
			mockedDataStore := &storetest.StorerMock{
				GetEditionFunc: func(ctx context.Context, dataset, edition, state string) (*models.EditionUpdate, error) {
					return &models.EditionUpdate{
						ID: "test",
						Next: &models.Edition{
							Edition: "unpublished-only",
							Links: &models.EditionUpdateLinks{
								LatestVersion: &models.LinkObject{
									ID: "1"}}},
					}, nil
				},

				UpsertEditionFunc: func(ctx context.Context, dataset, edition string, doc *models.EditionUpdate) error {
					return errs.ErrInternalServer
				},
			}

			s := Store{
				EnableDetachDataset: true,
				Storer:              mockedDataStore,
				Host:                testHost,
			}

			Convey("when confirmEdition is called again", func() {
				editionName := "unpublished-only"

				_, err := s.confirmEdition(context.Background(), testDatasetID, editionName, testInstanceID)

				Convey("then an internal server error is returned.", func() {
					So(err, ShouldEqual, errs.ErrVersionAlreadyExists)
				})
			})
		})
	}

	Convey("given an edition exists with a published version 10", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(ctx context.Context, dataset, edition, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "test",
					Next: &models.Edition{
						ID:      "test",
						Edition: "published-data",
						Links: &models.EditionUpdateLinks{
							LatestVersion: &models.LinkObject{
								ID:   "10",
								HRef: "example.com/datasets/10/editions/published-data/versions/10",
							},
							Dataset: &models.LinkObject{
								ID:   "10",
								HRef: "example.com/datasets/10",
							},
							Self: &models.LinkObject{
								HRef: "example.com/datasets/10/editions/published-data",
							},
						},
					},
					Current: &models.Edition{
						ID:      "test",
						Edition: "published-data",
						Links: &models.EditionUpdateLinks{
							LatestVersion: &models.LinkObject{
								ID:   "10",
								HRef: "example.com/datasets/10/editions/published-data/versions/10",
							},
							Dataset: &models.LinkObject{
								ID:   "10",
								HRef: "example.com/datasets/10",
							},
							Self: &models.LinkObject{
								HRef: "example.com/datasets/10/editions/published-data",
							},
						},
					},
				}, nil
			},
			UpsertEditionFunc: func(ctx context.Context, dataset, edition string, doc *models.EditionUpdate) error {
				return nil
			},
		}

		s := Store{
			Storer: mockedDataStore,
			Host:   testHost,
		}
		Convey("when confirmEdition is called", func() {
			edition, err := s.confirmEdition(ctx, testDatasetID, testEditionName, testInstanceID)

			Convey("then the edition is updated and the latest version ID is 11", func() {
				So(err, ShouldBeNil)
				So(edition, ShouldNotBeNil)

				So(edition.Current, ShouldNotBeNil)
				So(edition.Current.Links, ShouldNotBeNil)
				So(edition.Current.Links.LatestVersion, ShouldNotBeNil)
				So(edition.Current.Links.LatestVersion.ID, ShouldEqual, "10")

				So(edition.Next, ShouldNotBeNil)
				So(edition.Next.Links, ShouldNotBeNil)
				So(edition.Next.Links.LatestVersion, ShouldNotBeNil)
				So(edition.Next.Links.LatestVersion.ID, ShouldEqual, "11")
			})
		})
	})
}

func Test_ConfirmEditionReturnsError(t *testing.T) {
	Convey("given the datastore is unavailable", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(ctx context.Context, dataset, edition, state string) (*models.EditionUpdate, error) {
				return nil, errs.ErrInternalServer
			},
		}

		s := Store{
			Storer: mockedDataStore,
			Host:   testHost,
		}
		Convey("when confirmEdition is called", func() {
			_, err := s.confirmEdition(ctx, testDatasetID, testEditionName, testInstanceID)

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errs.ErrInternalServer)
			})
		})
	})

	Convey("given an invalid edition exists", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(ctx context.Context, dataset, edition, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "test",
					Current: &models.Edition{
						Links: &models.EditionUpdateLinks{
							LatestVersion: &models.LinkObject{
								ID: ""},
						},
					},
					Next: &models.Edition{
						Links: &models.EditionUpdateLinks{
							LatestVersion: &models.LinkObject{
								ID: ""},
						},
					},
				}, nil
			},
		}

		s := Store{
			Storer: mockedDataStore,
			Host:   testHost,
		}

		Convey("when confirmEdition is called", func() {
			_, err := s.confirmEdition(ctx, testDatasetID, testEditionName, testInstanceID)

			Convey("then updating links fails and an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, models.ErrEditionLinksInvalid)
			})
		})
	})

	Convey("given an edition exists with nil current doc", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(ctx context.Context, dataset, edition, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "test",
					Next: &models.Edition{
						Links: &models.EditionUpdateLinks{
							LatestVersion: &models.LinkObject{
								ID: ""},
						},
					},
				}, nil
			},
		}

		s := Store{
			Storer:              mockedDataStore,
			Host:                testHost,
			EnableDetachDataset: true,
		}

		Convey("when confirmEdition is called", func() {
			_, err := s.confirmEdition(ctx, testDatasetID, testEditionName, testInstanceID)

			Convey("then updating links fails and an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, models.ErrEditionLinksInvalid)
			})
		})
	})

	Convey("given an edition exists with nil next doc", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(ctx context.Context, dataset, edition, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "test",
					Current: &models.Edition{
						Links: &models.EditionUpdateLinks{
							LatestVersion: &models.LinkObject{
								ID: ""},
						},
					},
				}, nil
			},
		}

		s := Store{
			Storer:              mockedDataStore,
			Host:                testHost,
			EnableDetachDataset: true,
		}

		Convey("when confirmEdition is called", func() {
			_, err := s.confirmEdition(ctx, testDatasetID, testEditionName, testInstanceID)

			Convey("then updating links fails and an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, models.ErrEditionLinksInvalid)
			})
		})
	})

	Convey("given intermittent datastore failures", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(ctx context.Context, dataset, edition, state string) (*models.EditionUpdate, error) {
				return &models.EditionUpdate{
					ID: "test",
					Next: &models.Edition{
						ID:      "test",
						Edition: "unpublished-only",
						Links: &models.EditionUpdateLinks{
							LatestVersion: &models.LinkObject{
								ID:   "1",
								HRef: "example.com/datasets/1/editions/unpublished-only/versions/1",
							},
							Dataset: &models.LinkObject{
								ID:   "1",
								HRef: "example.com/datasets/1",
							},
							Self: &models.LinkObject{
								HRef: "example.com/datasets/1/editions/unpublished-only",
							},
						},
					},
					Current: &models.Edition{
						ID:      "test",
						Edition: "unpublished-only",
						Links: &models.EditionUpdateLinks{
							LatestVersion: &models.LinkObject{
								ID:   "1",
								HRef: "example.com/datasets/1/editions/unpublished-only/versions/1",
							},
							Dataset: &models.LinkObject{
								ID:   "1",
								HRef: "example.com/datasets/1",
							},
							Self: &models.LinkObject{
								HRef: "example.com/datasets/1/editions/unpublished-only",
							},
						},
					},
				}, nil
			},
			UpsertEditionFunc: func(ctx context.Context, dataset, edition string, doc *models.EditionUpdate) error {
				return errs.ErrInternalServer
			},
		}

		s := Store{
			Storer: mockedDataStore,
			Host:   testHost,
		}

		Convey("when confirmEdition is called and updating the datastore for the edition fails", func() {
			_, err := s.confirmEdition(ctx, testDatasetID, testEditionName, testInstanceID)

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errs.ErrInternalServer)
			})
		})
	})
}
