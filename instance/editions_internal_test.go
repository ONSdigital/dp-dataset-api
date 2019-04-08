package instance

import (
	"fmt"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	"github.com/ONSdigital/go-ns/audit/auditortest"
	. "github.com/smartystreets/goconvey/convey"
	"os"
	"strconv"
)

func Test_ConfirmEditionReturnsOK(t *testing.T) {

	Convey("given no edition exists", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(dataset, edition, state string) (*models.EditionUpdate, error) {
				return nil, errs.ErrEditionNotFound
			},
			UpsertEditionFunc: func(dataset, edition string, doc *models.EditionUpdate) error {
				return nil
			},
		}

		host := "example.com"
		s := Store{
			Storer:  mockedDataStore,
			Host:    host,
			Auditor: auditortest.New(),
		}

		Convey("when confirmEdition is called", func() {
			datasetID := "1234"
			editionName := "not-exist"
			instanceID := "new-instance-1234"

			edition, err := s.confirmEdition(ctx, datasetID, editionName, instanceID)

			Convey("then an edition is created and the version ID is 1", func() {
				So(edition, ShouldNotBeNil)
				So(err, ShouldBeNil)

				So(len(edition.ID), ShouldBeGreaterThan, 0)
				So(edition.Current, ShouldBeNil)
				So(edition.Next, ShouldNotBeNil)

				So(edition.Next, ShouldResemble, &models.Edition{
					Edition: editionName,
					State:   models.EditionConfirmedState,
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							ID:   datasetID,
							HRef: fmt.Sprintf("%s/datasets/%s", s.Host, datasetID),
						},
						Self: &models.LinkObject{
							HRef: fmt.Sprintf("%s/datasets/%s/editions/%s", s.Host, datasetID, editionName),
						},
						Versions: &models.LinkObject{
							HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions", s.Host, datasetID, editionName),
						},
						LatestVersion: &models.LinkObject{
							ID:   "1",
							HRef: fmt.Sprintf("%s/datasets/%s/editions/%s/versions/1", s.Host, datasetID, editionName),
						},
					},
				})

			})
		})
	})

	// TODO conditional test for feature flagged functionality. Will need tidying up eventually.
	featureEnvString := os.Getenv("FEATURE_DETACH_DATASET")
	featureOn, _ := strconv.ParseBool(featureEnvString)
	if featureOn {
		Convey("given an edition exists with 1 unpublished version", t, func() {
			mockedDataStore := &storetest.StorerMock{
				GetEditionFunc: func(dataset, edition, state string) (*models.EditionUpdate, error) {
					return &models.EditionUpdate{
						ID: "test",
						Next: &models.Edition{
							Edition: "unpublished-only",
							Links: &models.EditionUpdateLinks{
								LatestVersion: &models.LinkObject{
									ID: "1"}}},
					}, nil
				},

				UpsertEditionFunc: func(dataset, edition string, doc *models.EditionUpdate) error {
					return errs.ErrInternalServer
				},
			}

			host := "example.com"
			s := Store{
				Storer:  mockedDataStore,
				Host:    host,
				Auditor: auditortest.New(),
			}

			Convey("when confirmEdition is called again", func() {
				datasetID := "1234"
				editionName := "unpublished-only"
				instanceID := "new-instance-1234"

				_, err := s.confirmEdition(ctx, datasetID, editionName, instanceID)

				Convey("then an internal server error is returned.", func() {
					So(err, ShouldEqual, errs.ErrVersionAlreadyExists)
				})
			})
		})
	}

	Convey("given an edition exists with a published version 10", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(dataset, edition, state string) (*models.EditionUpdate, error) {
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
			UpsertEditionFunc: func(dataset, edition string, doc *models.EditionUpdate) error {
				return nil
			},
		}

		host := "example.com"
		s := Store{
			Storer:  mockedDataStore,
			Host:    host,
			Auditor: auditortest.New(),
		}
		Convey("when confirmEdition is called", func() {
			datasetID := "1234"
			editionName := "published-data"
			instanceID := "new-instance-1234"

			edition, err := s.confirmEdition(ctx, datasetID, editionName, instanceID)

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
			GetEditionFunc: func(dataset, edition, state string) (*models.EditionUpdate, error) {
				return nil, errs.ErrInternalServer
			},
		}

		host := "example.com"
		s := Store{
			Storer:  mockedDataStore,
			Host:    host,
			Auditor: auditortest.New(),
		}
		Convey("when confirmEdition is called", func() {
			datasetID := "1234"
			editionName := "failure"
			instanceID := "new-instance-1234"

			_, err := s.confirmEdition(ctx, datasetID, editionName, instanceID)

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errs.ErrInternalServer)
			})
		})
	})

	Convey("given an invalid edition exists", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(dataset, edition, state string) (*models.EditionUpdate, error) {
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

		host := "example.com"
		s := Store{
			Storer:  mockedDataStore,
			Host:    host,
			Auditor: auditortest.New(),
		}

		Convey("when confirmEdition is called", func() {
			datasetID := "1234"
			editionName := "failure"
			instanceID := "new-instance-1234"

			_, err := s.confirmEdition(ctx, datasetID, editionName, instanceID)

			Convey("then updating links fails and an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, models.ErrEditionLinksInvalid)
			})
		})
	})

	Convey("given intermittent datastore failures", t, func() {
		mockedDataStore := &storetest.StorerMock{
			GetEditionFunc: func(dataset, edition, state string) (*models.EditionUpdate, error) {
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
			UpsertEditionFunc: func(dataset, edition string, doc *models.EditionUpdate) error {
				return errs.ErrInternalServer
			},
		}

		host := "example.com"
		s := Store{
			Storer:  mockedDataStore,
			Host:    host,
			Auditor: auditortest.New(),
		}

		Convey("when confirmEdition is called and updating the datastore for the edition fails", func() {
			datasetID := "1234"
			editionName := "failure"
			instanceID := "new-instance-1234"

			_, err := s.confirmEdition(ctx, datasetID, editionName, instanceID)

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errs.ErrInternalServer)
			})
		})
	})
}
