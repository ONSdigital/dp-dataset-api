package utils

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"testing"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-net/v3/links"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	codeListAPIURL     = &neturl.URL{Scheme: "http", Host: "localhost:22400"}
	datasetAPIURL      = &neturl.URL{Scheme: "http", Host: "localhost:22000"}
	downloadServiceURL = &neturl.URL{Scheme: "http", Host: "localhost:23600"}
	importAPIURL       = &neturl.URL{Scheme: "http", Host: "localhost:21800"}
)

func TestMapVersionToEdition(t *testing.T) {
	Convey("Given a version model", t, func() {
		version := &models.Version{
			DatasetID:   "123",
			Edition:     "2023",
			ReleaseDate: "2023-10-01",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2023/versions/1",
					ID:   "1",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2023",
					ID:   "2023",
				},
			},
			State:              models.AssociatedState,
			Version:            1,
			LastUpdated:        time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
			Alerts:             &[]models.Alert{{Description: "Test alert"}},
			UsageNotes:         &[]models.UsageNote{{Note: "Test usage note"}},
			Distributions:      &[]models.Distribution{{Title: "Test distribution"}},
			QualityDesignation: "Test quality designation",
		}

		Convey("When the version is mapped to an edition", func() {
			edition := MapVersionToEdition(version)

			Convey("Then the edition should be mapped correctly", func() {
				So(edition.DatasetID, ShouldEqual, version.DatasetID)
				So(edition.Edition, ShouldEqual, version.Edition)
				So(edition.ReleaseDate, ShouldEqual, version.ReleaseDate)
				So(edition.Links.Dataset.HRef, ShouldEqual, version.Links.Dataset.HRef)
				So(edition.Links.Dataset.ID, ShouldEqual, version.Links.Dataset.ID)
				So(edition.Links.LatestVersion.HRef, ShouldEqual, version.Links.Version.HRef)
				So(edition.Links.LatestVersion.ID, ShouldEqual, version.Links.Version.ID)
				So(edition.Links.Self.HRef, ShouldEqual, version.Links.Edition.HRef)
				So(edition.Links.Self.ID, ShouldEqual, version.Links.Edition.ID)
				So(edition.Links.Versions.HRef, ShouldEqual, version.Links.Edition.HRef+"/versions")
				So(edition.State, ShouldEqual, version.State)
				So(edition.Version, ShouldEqual, version.Version)
				So(edition.LastUpdated, ShouldEqual, version.LastUpdated)
				So(edition.Alerts, ShouldResemble, version.Alerts)
				So(edition.UsageNotes, ShouldResemble, version.UsageNotes)
				So(edition.Distributions, ShouldResemble, version.Distributions)
				So(edition.QualityDesignation, ShouldEqual, version.QualityDesignation)
			})
		})
	})
}

func TestMapVersionsToEditions(t *testing.T) {
	Convey("Given a published and unpublished version", t, func() {
		publishedVersion := &models.Version{
			DatasetID:   "123",
			Edition:     "2023",
			ReleaseDate: "2023-10-01",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2023/versions/1",
					ID:   "1",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2023",
					ID:   "2023",
				},
			},
			Version:            1,
			LastUpdated:        time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
			Alerts:             &[]models.Alert{{Description: "Test alert"}},
			UsageNotes:         &[]models.UsageNote{{Note: "Test usage note"}},
			Distributions:      &[]models.Distribution{{Title: "Test distribution"}},
			QualityDesignation: "Test quality designation",
			State:              models.PublishedState,
		}

		unpublishedVersion := &models.Version{
			DatasetID:   "123",
			Edition:     "2023",
			ReleaseDate: "2023-10-01",
			Links: &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
					ID:   "123",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2023/versions/2",
					ID:   "2",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/2023",
					ID:   "2023",
				},
			},
			Version:            2,
			LastUpdated:        time.Date(2023, 9, 30, 12, 0, 0, 0, time.UTC),
			Alerts:             &[]models.Alert{{Description: "Test alert"}},
			UsageNotes:         &[]models.UsageNote{{Note: "Test usage note"}},
			Distributions:      &[]models.Distribution{{Title: "Test distribution"}},
			QualityDesignation: "Test quality designation",
			State:              models.AssociatedState,
		}

		Convey("When both versions are available", func() {
			edition, err := MapVersionsToEditionUpdate(publishedVersion, unpublishedVersion)

			Convey("Then the published version should be mapped to the 'Current' field", func() {
				So(err, ShouldBeNil)
				So(edition.Current.DatasetID, ShouldEqual, publishedVersion.DatasetID)
				So(edition.Current.Edition, ShouldEqual, publishedVersion.Edition)
				So(edition.Current.ReleaseDate, ShouldEqual, publishedVersion.ReleaseDate)
				So(edition.Current.Links.Dataset.HRef, ShouldEqual, publishedVersion.Links.Dataset.HRef)
				So(edition.Current.Links.Dataset.ID, ShouldEqual, publishedVersion.Links.Dataset.ID)
				So(edition.Current.Links.LatestVersion.HRef, ShouldEqual, publishedVersion.Links.Version.HRef)
				So(edition.Current.Links.LatestVersion.ID, ShouldEqual, publishedVersion.Links.Version.ID)
				So(edition.Current.Links.Self.HRef, ShouldEqual, publishedVersion.Links.Edition.HRef)
				So(edition.Current.Links.Self.ID, ShouldEqual, publishedVersion.Links.Edition.ID)
				So(edition.Current.Links.Versions.HRef, ShouldEqual, publishedVersion.Links.Edition.HRef+"/versions")
				So(edition.Current.Version, ShouldEqual, publishedVersion.Version)
				So(edition.Current.LastUpdated, ShouldEqual, publishedVersion.LastUpdated)
				So(edition.Current.Alerts, ShouldResemble, publishedVersion.Alerts)
				So(edition.Current.UsageNotes, ShouldResemble, publishedVersion.UsageNotes)
				So(edition.Current.Distributions, ShouldResemble, publishedVersion.Distributions)
				So(edition.Current.QualityDesignation, ShouldEqual, publishedVersion.QualityDesignation)
				So(edition.Current.State, ShouldEqual, publishedVersion.State)
			})

			Convey("And the unpublished version should be mapped to the 'Next' field", func() {
				So(edition.Next.DatasetID, ShouldEqual, unpublishedVersion.DatasetID)
				So(edition.Next.Edition, ShouldEqual, unpublishedVersion.Edition)
				So(edition.Next.ReleaseDate, ShouldEqual, unpublishedVersion.ReleaseDate)
				So(edition.Next.Links.Dataset.HRef, ShouldEqual, unpublishedVersion.Links.Dataset.HRef)
				So(edition.Next.Links.Dataset.ID, ShouldEqual, unpublishedVersion.Links.Dataset.ID)
				So(edition.Next.Links.LatestVersion.HRef, ShouldEqual, unpublishedVersion.Links.Version.HRef)
				So(edition.Next.Links.LatestVersion.ID, ShouldEqual, unpublishedVersion.Links.Version.ID)
				So(edition.Next.Links.Self.HRef, ShouldEqual, unpublishedVersion.Links.Edition.HRef)
				So(edition.Next.Links.Self.ID, ShouldEqual, unpublishedVersion.Links.Edition.ID)
				So(edition.Next.Links.Versions.HRef, ShouldEqual, unpublishedVersion.Links.Edition.HRef+"/versions")
				So(edition.Next.Version, ShouldEqual, unpublishedVersion.Version)
				So(edition.Next.LastUpdated, ShouldEqual, unpublishedVersion.LastUpdated)
				So(edition.Next.Alerts, ShouldResemble, unpublishedVersion.Alerts)
				So(edition.Next.UsageNotes, ShouldResemble, unpublishedVersion.UsageNotes)
				So(edition.Next.Distributions, ShouldResemble, unpublishedVersion.Distributions)
				So(edition.Next.QualityDesignation, ShouldEqual, unpublishedVersion.QualityDesignation)
				So(edition.Next.State, ShouldEqual, unpublishedVersion.State)
			})
		})

		Convey("When only the published version is available", func() {
			edition, err := MapVersionsToEditionUpdate(publishedVersion, nil)

			Convey("Then the published version should be mapped to both 'Current' and 'Next' fields", func() {
				So(err, ShouldBeNil)
				So(edition.Current.DatasetID, ShouldEqual, publishedVersion.DatasetID)
				So(edition.Current.Edition, ShouldEqual, publishedVersion.Edition)
				So(edition.Current.ReleaseDate, ShouldEqual, publishedVersion.ReleaseDate)
				So(edition.Current.Links.Dataset.HRef, ShouldEqual, publishedVersion.Links.Dataset.HRef)
				So(edition.Current.Links.Dataset.ID, ShouldEqual, publishedVersion.Links.Dataset.ID)
				So(edition.Current.Links.LatestVersion.HRef, ShouldEqual, publishedVersion.Links.Version.HRef)
				So(edition.Current.Links.LatestVersion.ID, ShouldEqual, publishedVersion.Links.Version.ID)
				So(edition.Current.Links.Self.HRef, ShouldEqual, publishedVersion.Links.Edition.HRef)
				So(edition.Current.Links.Self.ID, ShouldEqual, publishedVersion.Links.Edition.ID)
				So(edition.Current.Links.Versions.HRef, ShouldEqual, publishedVersion.Links.Edition.HRef+"/versions")
				So(edition.Current.Version, ShouldEqual, publishedVersion.Version)
				So(edition.Current.LastUpdated, ShouldEqual, publishedVersion.LastUpdated)
				So(edition.Current.Alerts, ShouldResemble, publishedVersion.Alerts)
				So(edition.Current.UsageNotes, ShouldResemble, publishedVersion.UsageNotes)
				So(edition.Current.Distributions, ShouldResemble, publishedVersion.Distributions)
				So(edition.Current.QualityDesignation, ShouldEqual, publishedVersion.QualityDesignation)
				So(edition.Current.State, ShouldEqual, publishedVersion.State)

				So(edition.Next.DatasetID, ShouldEqual, publishedVersion.DatasetID)
				So(edition.Next.Edition, ShouldEqual, publishedVersion.Edition)
				So(edition.Next.ReleaseDate, ShouldEqual, publishedVersion.ReleaseDate)
				So(edition.Next.Links.Dataset.HRef, ShouldEqual, publishedVersion.Links.Dataset.HRef)
				So(edition.Next.Links.Dataset.ID, ShouldEqual, publishedVersion.Links.Dataset.ID)
				So(edition.Next.Links.LatestVersion.HRef, ShouldEqual, publishedVersion.Links.Version.HRef)
				So(edition.Next.Links.LatestVersion.ID, ShouldEqual, publishedVersion.Links.Version.ID)
				So(edition.Next.Links.Self.HRef, ShouldEqual, publishedVersion.Links.Edition.HRef)
				So(edition.Next.Links.Self.ID, ShouldEqual, publishedVersion.Links.Edition.ID)
				So(edition.Next.Links.Versions.HRef, ShouldEqual, publishedVersion.Links.Edition.HRef+"/versions")
				So(edition.Next.Version, ShouldEqual, publishedVersion.Version)
				So(edition.Next.LastUpdated, ShouldEqual, publishedVersion.LastUpdated)
				So(edition.Next.Alerts, ShouldResemble, publishedVersion.Alerts)
				So(edition.Next.UsageNotes, ShouldResemble, publishedVersion.UsageNotes)
				So(edition.Next.Distributions, ShouldResemble, publishedVersion.Distributions)
				So(edition.Next.QualityDesignation, ShouldEqual, publishedVersion.QualityDesignation)
				So(edition.Next.State, ShouldEqual, publishedVersion.State)
			})
		})

		Convey("When only the unpublished version is available", func() {
			edition, err := MapVersionsToEditionUpdate(nil, unpublishedVersion)

			Convey("Then the unpublished version should be mapped to the 'Next' field", func() {
				So(err, ShouldBeNil)
				So(edition.Current, ShouldBeNil)
				So(edition.Next.DatasetID, ShouldEqual, unpublishedVersion.DatasetID)
				So(edition.Next.Edition, ShouldEqual, unpublishedVersion.Edition)
				So(edition.Next.ReleaseDate, ShouldEqual, unpublishedVersion.ReleaseDate)
				So(edition.Next.Links.Dataset.HRef, ShouldEqual, unpublishedVersion.Links.Dataset.HRef)
				So(edition.Next.Links.Dataset.ID, ShouldEqual, unpublishedVersion.Links.Dataset.ID)
				So(edition.Next.Links.LatestVersion.HRef, ShouldEqual, unpublishedVersion.Links.Version.HRef)
				So(edition.Next.Links.LatestVersion.ID, ShouldEqual, unpublishedVersion.Links.Version.ID)
				So(edition.Next.Links.Self.HRef, ShouldEqual, unpublishedVersion.Links.Edition.HRef)
				So(edition.Next.Links.Self.ID, ShouldEqual, unpublishedVersion.Links.Edition.ID)
				So(edition.Next.Links.Versions.HRef, ShouldEqual, unpublishedVersion.Links.Edition.HRef+"/versions")
				So(edition.Next.Version, ShouldEqual, unpublishedVersion.Version)
				So(edition.Next.LastUpdated, ShouldEqual, unpublishedVersion.LastUpdated)
				So(edition.Next.Alerts, ShouldResemble, unpublishedVersion.Alerts)
				So(edition.Next.UsageNotes, ShouldResemble, unpublishedVersion.UsageNotes)
				So(edition.Next.Distributions, ShouldResemble, unpublishedVersion.Distributions)
				So(edition.Next.QualityDesignation, ShouldEqual, unpublishedVersion.QualityDesignation)
				So(edition.Next.State, ShouldEqual, unpublishedVersion.State)
			})
		})

		Convey("When neither version is available", func() {
			edition, err := MapVersionsToEditionUpdate(nil, nil)

			Convey("Then the edition should be nil", func() {
				So(edition, ShouldBeNil)
			})

			Convey("And a version not found error should be returned", func() {
				So(err, ShouldEqual, errs.ErrVersionNotFound)
			})
		})
	})
}

// Copilot used to format test data and generate .So() statements

func TestRewriteDatasetsWithAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dataset updates", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the dataset links need rewriting", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "https://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "https://oldhost:1000/economy/inflationandpriceindices",
							},
						},
					},
					Next: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "https://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123/editions/time-series/versions/2",
								ID:   "2",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "https://oldhost:1000/economy/inflationandpriceindices",
							},
						},
					},
				},
			}

			items, err := RewriteDatasetsWithAuth(ctx, results, datasetLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Current.Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(items[0].Current.Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(items[0].Current.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				So(items[0].Current.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(items[0].Current.Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
				So(items[0].Next.Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(items[0].Next.Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(items[0].Next.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/2")
				So(items[0].Next.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(items[0].Next.Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		Convey("When the dataset links do not need rewriting", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "https://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "http://localhost:22000/economy/inflationandpriceindices",
							},
						},
					},
					Next: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "https://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/time-series/versions/2",
								ID:   "2",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "http://localhost:22000/economy/inflationandpriceindices",
							},
						},
					},
				},
			}

			items, err := RewriteDatasetsWithAuth(ctx, results, datasetLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Current.Links.AccessRights.HRef, ShouldEqual, results[0].Current.Links.AccessRights.HRef)
				So(items[0].Current.Links.Editions.HRef, ShouldEqual, results[0].Current.Links.Editions.HRef)
				So(items[0].Current.Links.LatestVersion.HRef, ShouldEqual, results[0].Current.Links.LatestVersion.HRef)
				So(items[0].Current.Links.Self.HRef, ShouldEqual, results[0].Current.Links.Self.HRef)
				So(items[0].Current.Links.Taxonomy.HRef, ShouldEqual, results[0].Current.Links.Taxonomy.HRef)
				So(items[0].Next.Links.AccessRights.HRef, ShouldEqual, results[0].Next.Links.AccessRights.HRef)
				So(items[0].Next.Links.Editions.HRef, ShouldEqual, results[0].Next.Links.Editions.HRef)
				So(items[0].Next.Links.LatestVersion.HRef, ShouldEqual, results[0].Next.Links.LatestVersion.HRef)
				So(items[0].Next.Links.Self.HRef, ShouldEqual, results[0].Next.Links.Self.HRef)
				So(items[0].Next.Links.Taxonomy.HRef, ShouldEqual, results[0].Next.Links.Taxonomy.HRef)
			})
		})

		Convey("When the dataset links are nil", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID: "123",
					},
					Next: &models.Dataset{
						ID: "123",
					},
				},
			}

			items, err := RewriteDatasetsWithAuth(ctx, results, datasetLinksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Current.Links, ShouldBeNil)
				So(items[0].Next.Links, ShouldBeNil)
			})
		})

		Convey("When the dataset links are empty", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID:    "123",
						Links: &models.DatasetLinks{},
					},
					Next: &models.Dataset{
						ID:    "123",
						Links: &models.DatasetLinks{},
					},
				},
			}

			items, err := RewriteDatasetsWithAuth(ctx, results, datasetLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Current.Links, ShouldResemble, &models.DatasetLinks{})
				So(items[0].Next.Links, ShouldResemble, &models.DatasetLinks{})
			})
		})

		Convey("When the datasets are empty", func() {
			results := []*models.DatasetUpdate{}

			items, err := RewriteDatasetsWithAuth(ctx, results, datasetLinksBuilder)

			Convey("Then the datasets should remain empty", func() {
				So(err, ShouldBeNil)
				So(items, ShouldBeEmpty)
			})
		})
	})
}

func TestRewriteDatasetsWithAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dataset updates", t, func() {
		Convey("When the 'current' dataset links are unable to be parsed", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "://oldhost:1000/economy/inflationandpriceindices",
							},
						},
					},
				},
			}

			items, err := RewriteDatasetsWithAuth(ctx, results, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the 'next' dataset links are unable to be parsed", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Next: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123/editions/time-series/versions/2",
								ID:   "2",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "://oldhost:1000/economy/inflationandpriceindices",
							},
						},
					},
				},
			}

			items, err := RewriteDatasetsWithAuth(ctx, results, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDatasetsWithoutAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dataset updates", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the dataset links need rewriting", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "https://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "https://oldhost:1000/economy/inflationandpriceindices",
							},
						},
					},
					Next: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "https://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123/editions/time-series/versions/2",
								ID:   "2",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "https://oldhost:1000/economy/inflationandpriceindices",
							},
						},
					},
				},
			}

			items, err := RewriteDatasetsWithoutAuth(ctx, results, datasetLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(items[0].Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(items[0].Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				So(items[0].Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(items[0].Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		Convey("When the dataset links do not need rewriting", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "https://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "http://localhost:22000/economy/inflationandpriceindices",
							},
						},
					},
					Next: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "https://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123/editions/time-series/versions/2",
								ID:   "2",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "http://localhost:22000/economy/inflationandpriceindices",
							},
						},
					},
				},
			}

			items, err := RewriteDatasetsWithoutAuth(ctx, results, datasetLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Links.AccessRights.HRef, ShouldEqual, results[0].Current.Links.AccessRights.HRef)
				So(items[0].Links.Editions.HRef, ShouldEqual, results[0].Current.Links.Editions.HRef)
				So(items[0].Links.LatestVersion.HRef, ShouldEqual, results[0].Current.Links.LatestVersion.HRef)
				So(items[0].Links.Self.HRef, ShouldEqual, results[0].Current.Links.Self.HRef)
				So(items[0].Links.Taxonomy.HRef, ShouldEqual, results[0].Current.Links.Taxonomy.HRef)
			})
		})

		Convey("When the dataset links are nil", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID: "123",
					},
					Next: &models.Dataset{
						ID: "123",
					},
				},
			}

			items, err := RewriteDatasetsWithoutAuth(ctx, results, datasetLinksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Links, ShouldBeNil)
			})
		})

		Convey("When the dataset links are empty", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID:    "123",
						Links: &models.DatasetLinks{},
					},
					Next: &models.Dataset{
						ID:    "123",
						Links: &models.DatasetLinks{},
					},
				},
			}

			items, err := RewriteDatasetsWithoutAuth(ctx, results, datasetLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "123")
				So(items[0].Links, ShouldResemble, &models.DatasetLinks{})
			})
		})

		Convey("When the datasets are empty", func() {
			results := []*models.DatasetUpdate{}

			items, err := RewriteDatasetsWithoutAuth(ctx, results, datasetLinksBuilder)

			Convey("Then the datasets should remain empty", func() {
				So(err, ShouldBeNil)
				So(items, ShouldBeEmpty)
			})
		})
	})
}

func TestRewriteDatasetsWithoutAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dataset updates", t, func() {
		Convey("When the dataset links are unable to be parsed", func() {
			results := []*models.DatasetUpdate{
				{
					ID: "123",
					Current: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "://oldhost:1000/economy/inflationandpriceindices",
							},
						},
					},
					Next: &models.Dataset{
						ID: "123",
						Links: &models.DatasetLinks{
							AccessRights: &models.LinkObject{
								HRef: "://oldhost:1000/accessrights",
							},
							Editions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123/editions",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123/editions/time-series/versions/2",
								ID:   "2",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/123",
							},
							Taxonomy: &models.LinkObject{
								HRef: "://oldhost:1000/economy/inflationandpriceindices",
							},
						},
					},
				},
			}

			items, err := RewriteDatasetsWithoutAuth(ctx, results, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDatasetWithAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a dataset update", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the dataset links need rewriting", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID: "123",
					Links: &models.DatasetLinks{
						AccessRights: &models.LinkObject{
							HRef: "https://oldhost:1000/accessrights",
						},
						Editions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/123/editions",
						},
						LatestVersion: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/123/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/123",
						},
						Taxonomy: &models.LinkObject{
							HRef: "https://oldhost:1000/economy/inflationandpriceindices",
						},
					},
				},
				Next: &models.Dataset{
					ID: "123",
					Links: &models.DatasetLinks{
						AccessRights: &models.LinkObject{
							HRef: "https://oldhost:1000/accessrights",
						},
						Editions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/123/editions",
						},
						LatestVersion: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/123/editions/time-series/versions/2",
							ID:   "2",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/123",
						},
						Taxonomy: &models.LinkObject{
							HRef: "https://oldhost:1000/economy/inflationandpriceindices",
						},
					},
				},
			}

			item, err := RewriteDatasetWithAuth(ctx, result, datasetLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Current.Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(item.Current.Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(item.Current.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				So(item.Current.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(item.Current.Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
				So(item.Next.Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(item.Next.Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(item.Next.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/2")
				So(item.Next.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(item.Next.Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		Convey("When the dataset links do not need rewriting", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID: "123",
					Links: &models.DatasetLinks{
						AccessRights: &models.LinkObject{
							HRef: "https://oldhost:1000/accessrights",
						},
						Editions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions",
						},
						LatestVersion: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
						},
						Taxonomy: &models.LinkObject{
							HRef: "http://localhost:22000/economy/inflationandpriceindices",
						},
					},
				},
				Next: &models.Dataset{
					ID: "123",
					Links: &models.DatasetLinks{
						AccessRights: &models.LinkObject{
							HRef: "https://oldhost:1000/accessrights",
						},
						Editions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions",
						},
						LatestVersion: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/time-series/versions/2",
							ID:   "2",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
						},
						Taxonomy: &models.LinkObject{
							HRef: "http://localhost:22000/economy/inflationandpriceindices",
						},
					},
				},
			}

			item, err := RewriteDatasetWithAuth(ctx, result, datasetLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Current.Links.AccessRights.HRef, ShouldEqual, result.Current.Links.AccessRights.HRef)
				So(item.Current.Links.Editions.HRef, ShouldEqual, result.Current.Links.Editions.HRef)
				So(item.Current.Links.LatestVersion.HRef, ShouldEqual, result.Current.Links.LatestVersion.HRef)
				So(item.Current.Links.Self.HRef, ShouldEqual, result.Current.Links.Self.HRef)
				So(item.Current.Links.Taxonomy.HRef, ShouldEqual, result.Current.Links.Taxonomy.HRef)
				So(item.Next.Links.AccessRights.HRef, ShouldEqual, result.Next.Links.AccessRights.HRef)
				So(item.Next.Links.Editions.HRef, ShouldEqual, result.Next.Links.Editions.HRef)
				So(item.Next.Links.LatestVersion.HRef, ShouldEqual, result.Next.Links.LatestVersion.HRef)
				So(item.Next.Links.Self.HRef, ShouldEqual, result.Next.Links.Self.HRef)
				So(item.Next.Links.Taxonomy.HRef, ShouldEqual, result.Next.Links.Taxonomy.HRef)
			})
		})

		Convey("When the dataset links are nil", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID: "123",
				},
				Next: &models.Dataset{
					ID: "123",
				},
			}

			item, err := RewriteDatasetWithAuth(ctx, result, datasetLinksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Current.Links, ShouldBeNil)
				So(item.Next.Links, ShouldBeNil)
			})
		})

		Convey("When the dataset links are empty", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID:    "123",
					Links: &models.DatasetLinks{},
				},
				Next: &models.Dataset{
					ID:    "123",
					Links: &models.DatasetLinks{},
				},
			}

			item, err := RewriteDatasetWithAuth(ctx, result, datasetLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Current.Links, ShouldResemble, &models.DatasetLinks{})
				So(item.Next.Links, ShouldResemble, &models.DatasetLinks{})
			})
		})
	})
}

func TestRewriteDatasetWithAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a dataset update", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the 'current' dataset links are unable to be parsed", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID: "123",
					Links: &models.DatasetLinks{
						AccessRights: &models.LinkObject{
							HRef: "://oldhost:1000/accessrights",
						},
						Editions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/123/editions",
						},
						LatestVersion: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/123/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/123",
						},
						Taxonomy: &models.LinkObject{
							HRef: "://oldhost:1000/economy/inflationandpriceindices",
						},
					},
				},
			}

			item, err := RewriteDatasetWithAuth(ctx, result, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the 'next' dataset links are unable to be parsed", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Next: &models.Dataset{
					ID: "123",
					Links: &models.DatasetLinks{
						AccessRights: &models.LinkObject{
							HRef: "://oldhost:1000/accessrights",
						},
						Editions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/123/editions",
						},
						LatestVersion: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/123/editions/time-series/versions/2",
							ID:   "2",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/123",
						},
						Taxonomy: &models.LinkObject{
							HRef: "://oldhost:1000/economy/inflationandpriceindices",
						},
					},
				},
			}

			item, err := RewriteDatasetWithAuth(ctx, result, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the dataset is empty", func() {
			result := &models.DatasetUpdate{}

			item, err := RewriteDatasetWithAuth(ctx, result, datasetLinksBuilder)

			Convey("Then we should get a dataset not found error", func() {
				So(err, ShouldEqual, errs.ErrDatasetNotFound)
				So(item, ShouldBeNil)
			})
		})

		Convey("When the dataset is nil", func() {
			item, err := RewriteDatasetWithAuth(ctx, nil, datasetLinksBuilder)

			Convey("Then we should get a dataset not found error", func() {
				So(err, ShouldEqual, errs.ErrDatasetNotFound)
				So(item, ShouldBeNil)
			})
		})
	})
}

func TestRewriteDatasetWithoutAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a dataset update", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the dataset links need rewriting", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID: "123",
					Links: &models.DatasetLinks{
						AccessRights: &models.LinkObject{
							HRef: "https://oldhost:1000/accessrights",
						},
						Editions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/123/editions",
						},
						LatestVersion: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/123/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/123",
						},
						Taxonomy: &models.LinkObject{
							HRef: "https://oldhost:1000/economy/inflationandpriceindices",
						},
					},
				},
				Next: &models.Dataset{
					ID: "123",
					Links: &models.DatasetLinks{
						AccessRights: &models.LinkObject{
							HRef: "https://oldhost:1000/accessrights",
						},
						Editions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/123/editions",
						},
						LatestVersion: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/123/editions/time-series/versions/2",
							ID:   "2",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/123",
						},
						Taxonomy: &models.LinkObject{
							HRef: "https://oldhost:1000/economy/inflationandpriceindices",
						},
					},
				},
			}

			item, err := RewriteDatasetWithoutAuth(ctx, result, datasetLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Links.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(item.Links.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(item.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				So(item.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(item.Links.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		Convey("When the dataset links do not need rewriting", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID: "123",
					Links: &models.DatasetLinks{
						AccessRights: &models.LinkObject{
							HRef: "https://oldhost:1000/accessrights",
						},
						Editions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions",
						},
						LatestVersion: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
						},
						Taxonomy: &models.LinkObject{
							HRef: "http://localhost:22000/economy/inflationandpriceindices",
						},
					},
				},
				Next: &models.Dataset{
					ID: "123",
					Links: &models.DatasetLinks{
						AccessRights: &models.LinkObject{
							HRef: "https://oldhost:1000/accessrights",
						},
						Editions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions",
						},
						LatestVersion: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123/editions/time-series/versions/2",
							ID:   "2",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/123",
						},
						Taxonomy: &models.LinkObject{
							HRef: "http://localhost:22000/economy/inflationandpriceindices",
						},
					},
				},
			}

			item, err := RewriteDatasetWithoutAuth(ctx, result, datasetLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Links.AccessRights.HRef, ShouldEqual, result.Current.Links.AccessRights.HRef)
				So(item.Links.Editions.HRef, ShouldEqual, result.Current.Links.Editions.HRef)
				So(item.Links.LatestVersion.HRef, ShouldEqual, result.Current.Links.LatestVersion.HRef)
				So(item.Links.Self.HRef, ShouldEqual, result.Current.Links.Self.HRef)
				So(item.Links.Taxonomy.HRef, ShouldEqual, result.Current.Links.Taxonomy.HRef)
			})
		})

		Convey("When the dataset links are nil", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID: "123",
				},
				Next: &models.Dataset{
					ID: "123",
				},
			}

			item, err := RewriteDatasetWithoutAuth(ctx, result, datasetLinksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Links, ShouldBeNil)
			})
		})

		Convey("When the dataset links are empty", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID:    "123",
					Links: &models.DatasetLinks{},
				},
				Next: &models.Dataset{
					ID:    "123",
					Links: &models.DatasetLinks{},
				},
			}

			item, err := RewriteDatasetWithoutAuth(ctx, result, datasetLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "123")
				So(item.Links, ShouldResemble, &models.DatasetLinks{})
			})
		})
	})
}

func TestRewriteDatasetWithoutAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a dataset update", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the dataset links are unable to be parsed", func() {
			result := &models.DatasetUpdate{
				ID: "123",
				Current: &models.Dataset{
					ID: "123",
					Links: &models.DatasetLinks{
						AccessRights: &models.LinkObject{
							HRef: "://oldhost:1000/accessrights",
						},
						Editions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/123/editions",
						},
						LatestVersion: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/123/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/123",
						},
						Taxonomy: &models.LinkObject{
							HRef: "://oldhost:1000/economy/inflationandpriceindices",
						},
					},
				},
				Next: &models.Dataset{
					ID: "123",
					Links: &models.DatasetLinks{
						AccessRights: &models.LinkObject{
							HRef: "://oldhost:1000/accessrights",
						},
						Editions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/123/editions",
						},
						LatestVersion: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/123/editions/time-series/versions/2",
							ID:   "2",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/123",
						},
						Taxonomy: &models.LinkObject{
							HRef: "://oldhost:1000/economy/inflationandpriceindices",
						},
					},
				},
			}

			item, err := RewriteDatasetWithoutAuth(ctx, result, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the dataset is empty", func() {
			result := &models.DatasetUpdate{}

			item, err := RewriteDatasetWithoutAuth(ctx, result, datasetLinksBuilder)

			Convey("Then we should get a dataset not found error", func() {
				So(err, ShouldEqual, errs.ErrDatasetNotFound)
				So(item, ShouldBeNil)
			})
		})

		Convey("When the dataset is nil", func() {
			item, err := RewriteDatasetWithoutAuth(ctx, nil, datasetLinksBuilder)

			Convey("Then we should get a dataset not found error", func() {
				So(err, ShouldEqual, errs.ErrDatasetNotFound)
				So(item, ShouldBeNil)
			})
		})
	})
}

func TestRewriteDatasetLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of dataset links", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the dataset links need rewriting", func() {
			datasetLinks := &models.DatasetLinks{
				AccessRights: &models.LinkObject{
					HRef: "https://oldhost:1000/accessrights",
				},
				Editions: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/123/editions",
				},
				LatestVersion: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/123/editions/time-series/versions/1",
					ID:   "1",
				},
				Self: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/123",
				},
				Taxonomy: &models.LinkObject{
					HRef: "https://oldhost:1000/economy/inflationandpriceindices",
				},
			}

			err := RewriteDatasetLinks(ctx, datasetLinks, datasetLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(datasetLinks.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(datasetLinks.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(datasetLinks.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				So(datasetLinks.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(datasetLinks.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		Convey("When the dataset links do not need rewriting", func() {
			datasetLinks := &models.DatasetLinks{
				AccessRights: &models.LinkObject{
					HRef: "https://oldhost:1000/accessrights",
				},
				Editions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions",
				},
				LatestVersion: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123/editions/time-series/versions/1",
					ID:   "1",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/123",
				},
				Taxonomy: &models.LinkObject{
					HRef: "http://localhost:22000/economy/inflationandpriceindices",
				},
			}

			err := RewriteDatasetLinks(ctx, datasetLinks, datasetLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(datasetLinks.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(datasetLinks.Editions.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions")
				So(datasetLinks.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/123/editions/time-series/versions/1")
				So(datasetLinks.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/123")
				So(datasetLinks.Taxonomy.HRef, ShouldEqual, "http://localhost:22000/economy/inflationandpriceindices")
			})
		})

		Convey("When the dataset links are empty", func() {
			datasetLinks := &models.DatasetLinks{}

			err := RewriteDatasetLinks(ctx, datasetLinks, datasetLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(datasetLinks, ShouldResemble, &models.DatasetLinks{})
			})
		})

		Convey("When the dataset links are nil", func() {
			err := RewriteDatasetLinks(ctx, nil, datasetLinksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteDatasetLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of dataset links", t, func() {
		Convey("When the dataset links are unable to be parsed", func() {
			datasetLinks := &models.DatasetLinks{
				AccessRights: &models.LinkObject{
					HRef: "://oldhost:1000/accessrights",
				},
				Editions: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/123/editions",
				},
				LatestVersion: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/123/editions/time-series/versions/1",
					ID:   "1",
				},
				Self: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/123",
				},
				Taxonomy: &models.LinkObject{
					HRef: "://oldhost:1000/economy/inflationandpriceindices",
				},
			}

			err := RewriteDatasetLinks(ctx, datasetLinks, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDimensions_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dimensions", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		codeListLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, codeListAPIURL)
		Convey("When the dimension links need rewriting", func() {
			results := []models.Dimension{
				{
					Label: "Aggregate",
					Links: models.DimensionLink{
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Options: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
							ID:   "aggregate",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionLink{
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/uk-only",
							ID:   "uk-only",
						},
						Options: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
							ID:   "geography",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "geography",
				},
			}

			items, err := RewriteDimensions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].Label, ShouldEqual, "Aggregate")
				So(items[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				So(items[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				So(items[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[0].Name, ShouldEqual, "aggregate")
				So(items[1].Label, ShouldEqual, "Geography")
				So(items[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				So(items[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				So(items[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[1].Name, ShouldEqual, "geography")
			})
		})

		Convey("When the dimension links do not need rewriting", func() {
			results := []models.Dimension{
				{
					Label: "Aggregate",
					Links: models.DimensionLink{
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Options: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
							ID:   "aggregate",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionLink{
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/uk-only",
							ID:   "uk-only",
						},
						Options: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
							ID:   "geography",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "geography",
				},
			}

			items, err := RewriteDimensions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(items[0].Label, ShouldEqual, "Aggregate")
				So(items[0].Links.CodeList.HRef, ShouldEqual, results[0].Links.CodeList.HRef)
				So(items[0].Links.Options.HRef, ShouldEqual, results[0].Links.Options.HRef)
				So(items[0].Links.Version.HRef, ShouldEqual, results[0].Links.Version.HRef)
				So(items[0].Name, ShouldEqual, "aggregate")
				So(items[1].Label, ShouldEqual, "Geography")
				So(items[1].Links.CodeList.HRef, ShouldEqual, results[1].Links.CodeList.HRef)
				So(items[1].Links.Options.HRef, ShouldEqual, results[1].Links.Options.HRef)
				So(items[1].Links.Version.HRef, ShouldEqual, results[1].Links.Version.HRef)
				So(items[1].Name, ShouldEqual, "geography")
			})
		})

		Convey("When each dimension needs its link rewritten", func() {
			results := []models.Dimension{
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "https://oldhost:1000/code-lists/mmm-yy",
					ID:   "mmm-yy",
					Name: "time",
				},
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "https://oldhost:1000/code-lists/uk-only",
					ID:   "uk-only",
					Name: "geography",
				},
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
					Name: "aggregate",
				},
			}

			items, err := RewriteDimensions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].HRef, ShouldEqual, "http://localhost:22400/code-lists/mmm-yy")
				So(items[1].HRef, ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				So(items[2].HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
			})
		})

		Convey("When each dimemsion doesn't need its link rewritten", func() {
			results := []models.Dimension{
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "http://localhost:22400/code-lists/mmm-yy",
					ID:   "mmm-yy",
					Name: "time",
				},
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "http://localhost:22400/code-lists/uk-only",
					ID:   "uk-only",
					Name: "geography",
				},
				{
					Links: models.DimensionLink{
						CodeList: models.LinkObject{},
						Options:  models.LinkObject{},
						Version:  models.LinkObject{},
					},
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
					Name: "aggregate",
				},
			}

			items, err := RewriteDimensions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(items[0].HRef, ShouldEqual, results[0].HRef)
				So(items[1].HRef, ShouldEqual, results[1].HRef)
				So(items[2].HRef, ShouldEqual, results[2].HRef)
			})
		})

		Convey("When the dimension links are empty", func() {
			results := []models.Dimension{
				{
					Label: "Aggregate",
					Links: models.DimensionLink{},
					Name:  "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionLink{},
					Name:  "geography",
				},
			}

			items, err := RewriteDimensions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items[0].Label, ShouldEqual, "Aggregate")
				So(items[0].Links, ShouldResemble, models.DimensionLink{})
				So(items[0].Name, ShouldEqual, "aggregate")
				So(items[1].Label, ShouldEqual, "Geography")
				So(items[1].Links, ShouldResemble, models.DimensionLink{})
				So(items[1].Name, ShouldEqual, "geography")
			})
		})

		Convey("When the dimensions are nil", func() {
			items, err := RewriteDimensions(ctx, nil, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the dimensions should remain nil", func() {
				So(err, ShouldBeNil)
				So(items, ShouldBeNil)
			})
		})

		Convey("When the dimensions are empty", func() {
			items, err := RewriteDimensions(ctx, []models.Dimension{}, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items, ShouldResemble, []models.Dimension{})
			})
		})
	})
}

func TestRewriteDimensions_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dimensions", t, func() {
		Convey("When the dimension links are unable to be parsed", func() {
			results := []models.Dimension{
				{
					Label: "Aggregate",
					Links: models.DimensionLink{
						CodeList: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Options: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
							ID:   "aggregate",
						},
						Version: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionLink{
						CodeList: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/uk-only",
							ID:   "uk-only",
						},
						Options: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
							ID:   "geography",
						},
						Version: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "geography",
				},
			}

			items, err := RewriteDimensions(ctx, results, nil, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDimensionLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of dimension links", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		codeListLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, codeListAPIURL)
		Convey("When the dimension links need rewriting", func() {
			dimensionLinks := models.DimensionLink{
				CodeList: models.LinkObject{
					HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Options: models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
					ID:   "aggregate",
				},
				Version: models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionLinks(ctx, &dimensionLinks, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(dimensionLinks.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				So(dimensionLinks.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				So(dimensionLinks.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the dimension links do not need rewriting", func() {
			dimensionLinks := models.DimensionLink{
				CodeList: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Options: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
					ID:   "aggregate",
				},
				Version: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionLinks(ctx, &dimensionLinks, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(dimensionLinks.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				So(dimensionLinks.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				So(dimensionLinks.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the dimension links are empty", func() {
			dimensionLinks := models.DimensionLink{}

			err := RewriteDimensionLinks(ctx, &dimensionLinks, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(dimensionLinks, ShouldResemble, models.DimensionLink{})
			})
		})

		Convey("When the dimension links are nil", func() {
			err := RewriteDimensionLinks(ctx, nil, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteDimensionLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of dimension links", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		codeListLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, codeListAPIURL)
		Convey("When the Code List link is unable to be parsed", func() {
			dimensionLinks := models.DimensionLink{
				CodeList: models.LinkObject{
					HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Options: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
					ID:   "aggregate",
				},
				Version: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionLinks(ctx, &dimensionLinks, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the Options link is unable to be parsed", func() {
			dimensionLinks := models.DimensionLink{
				CodeList: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Options: models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
					ID:   "aggregate",
				},
				Version: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionLinks(ctx, &dimensionLinks, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the Version link is unable to be parsed", func() {
			dimensionLinks := models.DimensionLink{
				CodeList: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Options: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
					ID:   "aggregate",
				},
				Version: models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionLinks(ctx, &dimensionLinks, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewritePublicDimensionOptions_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of public dimension options", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		codeListLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, codeListAPIURL)
		Convey("When the public dimension options need rewriting", func() {
			results := []*models.PublicDimensionOption{
				{
					Label: "Aggregate",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Code: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid/codes/cpih1dim1A0",
							ID:   "aggregate",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/uk-only",
							ID:   "uk-only",
						},
						Code: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/uk-only/codes/K02000001",
							ID:   "geography",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "geography",
				},
				{
					Label: "Time",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Code: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/mmm-yy/codes/Apr-00",
							ID:   "time",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "time",
				},
			}

			items, err := RewritePublicDimensionOptions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].Label, ShouldEqual, "Aggregate")
				So(items[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				So(items[0].Links.Code.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1A0")
				So(items[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[0].Name, ShouldEqual, "aggregate")
				So(items[1].Label, ShouldEqual, "Geography")
				So(items[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				So(items[1].Links.Code.HRef, ShouldEqual, "http://localhost:22400/code-lists/uk-only/codes/K02000001")
				So(items[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[1].Name, ShouldEqual, "geography")
				So(items[2].Label, ShouldEqual, "Time")
				So(items[2].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/mmm-yy")
				So(items[2].Links.Code.HRef, ShouldEqual, "http://localhost:22400/code-lists/mmm-yy/codes/Apr-00")
				So(items[2].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[2].Name, ShouldEqual, "time")
			})
		})

		Convey("When the public dimension options do not need rewriting", func() {
			results := []*models.PublicDimensionOption{
				{
					Label: "Aggregate",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Code: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1A0",
							ID:   "aggregate",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/uk-only",
							ID:   "uk-only",
						},
						Code: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/uk-only/codes/K02000001",
							ID:   "geography",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "geography",
				},
				{
					Label: "Time",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Code: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/mmm-yy/codes/Apr-00",
							ID:   "time",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "time",
				},
			}

			items, err := RewritePublicDimensionOptions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(items[0].Label, ShouldEqual, "Aggregate")
				So(items[0].Links.CodeList.HRef, ShouldEqual, results[0].Links.CodeList.HRef)
				So(items[0].Links.Code.HRef, ShouldEqual, results[0].Links.Code.HRef)
				So(items[0].Links.Version.HRef, ShouldEqual, results[0].Links.Version.HRef)
				So(items[0].Name, ShouldEqual, "aggregate")
				So(items[1].Label, ShouldEqual, "Geography")
				So(items[1].Links.CodeList.HRef, ShouldEqual, results[1].Links.CodeList.HRef)
				So(items[1].Links.Code.HRef, ShouldEqual, results[1].Links.Code.HRef)
				So(items[1].Links.Version.HRef, ShouldEqual, results[1].Links.Version.HRef)
				So(items[1].Name, ShouldEqual, "geography")
				So(items[2].Label, ShouldEqual, "Time")
				So(items[2].Links.CodeList.HRef, ShouldEqual, results[2].Links.CodeList.HRef)
				So(items[2].Links.Code.HRef, ShouldEqual, results[2].Links.Code.HRef)
				So(items[2].Links.Version.HRef, ShouldEqual, results[2].Links.Version.HRef)
				So(items[2].Name, ShouldEqual, "time")
			})
		})

		Convey("When the public dimension options are nil", func() {
			items, err := RewritePublicDimensionOptions(ctx, nil, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the public dimension options should remain nil", func() {
				So(err, ShouldBeNil)
				So(items, ShouldBeNil)
			})
		})

		Convey("When the public dimension options are empty", func() {
			items, err := RewritePublicDimensionOptions(ctx, []*models.PublicDimensionOption{}, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items, ShouldResemble, []*models.PublicDimensionOption{})
			})
		})

		Convey("When the public dimension option links are empty", func() {
			results := []*models.PublicDimensionOption{
				{
					Label: "Aggregate",
					Links: models.DimensionOptionLinks{},
					Name:  "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionOptionLinks{},
					Name:  "geography",
				},
			}

			items, err := RewritePublicDimensionOptions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items[0].Label, ShouldEqual, "Aggregate")
				So(items[0].Links, ShouldResemble, models.DimensionOptionLinks{})
				So(items[0].Name, ShouldEqual, "aggregate")
				So(items[1].Label, ShouldEqual, "Geography")
				So(items[1].Links, ShouldResemble, models.DimensionOptionLinks{})
				So(items[1].Name, ShouldEqual, "geography")
			})
		})
	})
}

func TestRewritePublicDimensionOptions_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of public dimension options", t, func() {
		Convey("When the public dimension options are unable to be parsed", func() {
			results := []*models.PublicDimensionOption{
				{
					Label: "Aggregate",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Code: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/cpih1dim1aggid/codes/cpih1dim1A0",
							ID:   "aggregate",
						},
						Version: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "aggregate",
				},
				{
					Label: "Geography",
					Links: models.DimensionOptionLinks{
						CodeList: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/uk-only",
							ID:   "uk-only",
						},
						Code: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/uk-only/codes/K02000001",
							ID:   "geography",
						},
						Version: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name: "geography",
				},
			}

			items, err := RewritePublicDimensionOptions(ctx, results, nil, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDimensionOptions_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dimension options", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		codeListLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, codeListAPIURL)
		Convey("When the dimension options need rewriting", func() {
			results := []*models.DimensionOption{
				{
					Label: "May-89",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/mmm-yy/codes/May-89",
							ID:   "May-89",
						},
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "time",
					NodeID: "_37abc12d_time_May-89",
					Option: "May-89",
				},
				{
					Label: "01.1 Food",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
							ID:   "cpih1dim1G10100",
						},
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "aggregate",
					NodeID: "_37abc12d_aggregate_cpih1dim1G10100",
					Option: "cpih1dim1G10100",
				},
				{
					Label: "Mar-02",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/mmm-yy/codes/Mar-02",
							ID:   "Mar-02",
						},
						CodeList: models.LinkObject{
							HRef: "https://oldhost:1000/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Version: models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "time",
					NodeID: "_37abc12d_time_Mar-02",
					Option: "Mar-02",
				},
			}

			err := RewriteDimensionOptions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(results[0].Label, ShouldEqual, "May-89")
				So(results[0].Links.Code.HRef, ShouldEqual, "http://localhost:22400/code-lists/mmm-yy/codes/May-89")
				So(results[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/mmm-yy")
				So(results[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[0].Name, ShouldEqual, "time")
				So(results[0].NodeID, ShouldEqual, "_37abc12d_time_May-89")
				So(results[0].Option, ShouldEqual, "May-89")
				So(results[1].Label, ShouldEqual, "01.1 Food")
				So(results[1].Links.Code.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100")
				So(results[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				So(results[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[1].Name, ShouldEqual, "aggregate")
				So(results[1].NodeID, ShouldEqual, "_37abc12d_aggregate_cpih1dim1G10100")
				So(results[1].Option, ShouldEqual, "cpih1dim1G10100")
				So(results[2].Label, ShouldEqual, "Mar-02")
				So(results[2].Links.Code.HRef, ShouldEqual, "http://localhost:22400/code-lists/mmm-yy/codes/Mar-02")
				So(results[2].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/mmm-yy")
				So(results[2].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[2].Name, ShouldEqual, "time")
				So(results[2].NodeID, ShouldEqual, "_37abc12d_time_Mar-02")
				So(results[2].Option, ShouldEqual, "Mar-02")
			})
		})

		Convey("When the dimension options do not need rewriting", func() {
			results := []*models.DimensionOption{
				{
					Label: "May-89",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/mmm-yy/codes/May-89",
							ID:   "May-89",
						},
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "time",
					NodeID: "_37abc12d_time_May-89",
					Option: "May-89",
				},
				{
					Label: "01.1 Food",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
							ID:   "cpih1dim1G10100",
						},
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "aggregate",
					NodeID: "_37abc12d_aggregate_cpih1dim1G10100",
					Option: "cpih1dim1G10100",
				},
				{
					Label: "Mar-02",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/mmm-yy/codes/Mar-02",
							ID:   "Mar-02",
						},
						CodeList: models.LinkObject{
							HRef: "http://localhost:22400/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Version: models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "time",
					NodeID: "_37abc12d_time_Mar-02",
					Option: "Mar-02",
				},
			}

			err := RewriteDimensionOptions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(results[0].Label, ShouldEqual, "May-89")
				So(results[0].Links.Code.HRef, ShouldEqual, results[0].Links.Code.HRef)
				So(results[0].Links.CodeList.HRef, ShouldEqual, results[0].Links.CodeList.HRef)
				So(results[0].Links.Version.HRef, ShouldEqual, results[0].Links.Version.HRef)
				So(results[0].Name, ShouldEqual, "time")
				So(results[0].NodeID, ShouldEqual, "_37abc12d_time_May-89")
				So(results[0].Option, ShouldEqual, "May-89")
				So(results[1].Label, ShouldEqual, "01.1 Food")
				So(results[1].Links.Code.HRef, ShouldEqual, results[1].Links.Code.HRef)
				So(results[1].Links.CodeList.HRef, ShouldEqual, results[1].Links.CodeList.HRef)
				So(results[1].Links.Version.HRef, ShouldEqual, results[1].Links.Version.HRef)
				So(results[1].Name, ShouldEqual, "aggregate")
				So(results[1].NodeID, ShouldEqual, "_37abc12d_aggregate_cpih1dim1G10100")
				So(results[1].Option, ShouldEqual, "cpih1dim1G10100")
				So(results[2].Label, ShouldEqual, "Mar-02")
				So(results[2].Links.Code.HRef, ShouldEqual, results[2].Links.Code.HRef)
				So(results[2].Links.CodeList.HRef, ShouldEqual, results[2].Links.CodeList.HRef)
				So(results[2].Links.Version.HRef, ShouldEqual, results[2].Links.Version.HRef)
				So(results[2].Name, ShouldEqual, "time")
				So(results[2].NodeID, ShouldEqual, "_37abc12d_time_Mar-02")
				So(results[2].Option, ShouldEqual, "Mar-02")
			})
		})

		Convey("When the dimension options are nil", func() {
			err := RewriteDimensionOptions(ctx, nil, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the dimension options should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When the dimension options are empty", func() {
			err := RewriteDimensionOptions(ctx, []*models.DimensionOption{}, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When the dimension option links are empty", func() {
			results := []*models.DimensionOption{
				{
					Label:  "May-89",
					Links:  models.DimensionOptionLinks{},
					Name:   "time",
					NodeID: "_37abc12d_time_May-89",
					Option: "May-89",
				},
				{
					Label:  "01.1 Food",
					Links:  models.DimensionOptionLinks{},
					Name:   "aggregate",
					NodeID: "_37abc12d_aggregate_cpih1dim1G10100",
					Option: "cpih1dim1G10100",
				},
			}

			err := RewriteDimensionOptions(ctx, results, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(results[0].Label, ShouldEqual, "May-89")
				So(results[0].Links, ShouldResemble, models.DimensionOptionLinks{})
				So(results[0].Name, ShouldEqual, "time")
				So(results[0].NodeID, ShouldEqual, "_37abc12d_time_May-89")
				So(results[0].Option, ShouldEqual, "May-89")
				So(results[1].Label, ShouldEqual, "01.1 Food")
				So(results[1].Links, ShouldResemble, models.DimensionOptionLinks{})
				So(results[1].Name, ShouldEqual, "aggregate")
				So(results[1].NodeID, ShouldEqual, "_37abc12d_aggregate_cpih1dim1G10100")
				So(results[1].Option, ShouldEqual, "cpih1dim1G10100")
			})
		})
	})
}

func TestRewriteDimensionOptions_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of dimension options", t, func() {
		Convey("When the dimension options are unable to be parsed", func() {
			results := []*models.DimensionOption{
				{
					Label: "May-89",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/mmm-yy/codes/May-89",
							ID:   "May-89",
						},
						CodeList: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/mmm-yy",
							ID:   "mmm-yy",
						},
						Version: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "time",
					NodeID: "_37abc12d_time_May-89",
					Option: "May-89",
				},
				{
					Label: "01.1 Food",
					Links: models.DimensionOptionLinks{
						Code: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
							ID:   "cpih1dim1G10100",
						},
						CodeList: models.LinkObject{
							HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
						},
						Version: models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
						},
					},
					Name:   "aggregate",
					NodeID: "_37abc12d_aggregate_cpih1dim1G10100",
					Option: "cpih1dim1G10100",
				},
			}

			err := RewriteDimensionOptions(ctx, results, nil, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDimensionOptionLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of dimension option links", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		codeListLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, codeListAPIURL)
		Convey("When the dimension option links need rewriting", func() {
			dimensionOptionLinks := models.DimensionOptionLinks{
				Code: models.LinkObject{
					HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
					ID:   "cpih1dim1G10100",
				},
				CodeList: models.LinkObject{
					HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Version: models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionOptionLinks(ctx, &dimensionOptionLinks, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(dimensionOptionLinks.Code.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100")
				So(dimensionOptionLinks.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				So(dimensionOptionLinks.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the dimension option links do not need rewriting", func() {
			dimensionOptionLinks := models.DimensionOptionLinks{
				Code: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
					ID:   "cpih1dim1G10100",
				},
				CodeList: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Version: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionOptionLinks(ctx, &dimensionOptionLinks, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(dimensionOptionLinks.Code.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100")
				So(dimensionOptionLinks.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				So(dimensionOptionLinks.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the dimension option links are empty", func() {
			dimensionOptionLinks := models.DimensionOptionLinks{}

			err := RewriteDimensionOptionLinks(ctx, &dimensionOptionLinks, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(dimensionOptionLinks, ShouldResemble, models.DimensionOptionLinks{})
			})
		})

		Convey("When the dimension option links are nil", func() {
			err := RewriteDimensionOptionLinks(ctx, nil, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteDimensionOptionLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of dimension option links", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		codeListLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, codeListAPIURL)
		Convey("When the Code link is unable to be parsed", func() {
			dimensionOptionLinks := models.DimensionOptionLinks{
				Code: models.LinkObject{
					HRef: "://oldhost:1000/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
					ID:   "cpih1dim1G10100",
				},
				CodeList: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Version: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionOptionLinks(ctx, &dimensionOptionLinks, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the CodeList link is unable to be parsed", func() {
			dimensionOptionLinks := models.DimensionOptionLinks{
				Code: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
					ID:   "cpih1dim1G10100",
				},
				CodeList: models.LinkObject{
					HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Version: models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionOptionLinks(ctx, &dimensionOptionLinks, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the Version link is unable to be parsed", func() {
			dimensionOptionLinks := models.DimensionOptionLinks{
				Code: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid/codes/cpih1dim1G10100",
					ID:   "cpih1dim1G10100",
				},
				CodeList: models.LinkObject{
					HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
					ID:   "cpih1dim1aggid",
				},
				Version: models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteDimensionOptionLinks(ctx, &dimensionOptionLinks, datasetLinksBuilder, codeListLinksBuilder)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteEditionsWithAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of edition updates", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the edition update links and distributions need rewriting", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						Distributions: &[]models.Distribution{
							{
								Title:       "Distribution 1",
								Format:      "CSV",
								MediaType:   "text/csv",
								DownloadURL: "/cpih01/time-series/1/filename.csv",
								ByteSize:    10000,
							},
							{
								Title:       "Distribution 2",
								Format:      "XLSX",
								MediaType:   "text/xlsx",
								DownloadURL: "/cpih01/time-series/1/filename.xlsx",
								ByteSize:    20000,
							},
						},
						State: "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						Distributions: &[]models.Distribution{
							{
								Title:       "Distribution 1",
								Format:      "CSV",
								MediaType:   "text/csv",
								DownloadURL: "/cpih01/time-series/1/filename.csv",
								ByteSize:    10000,
							},
							{
								Title:       "Distribution 2",
								Format:      "XLSX",
								MediaType:   "text/xlsx",
								DownloadURL: "/cpih01/time-series/1/filename.xlsx",
								ByteSize:    20000,
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithAuth(ctx, results, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(items[0].Current.Edition, ShouldEqual, "time-series")
				So(items[0].Current.Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(items[0].Current.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[0].Current.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(items[0].Current.Links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				So((*items[0].Current.Distributions)[0].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv")
				So((*items[0].Current.Distributions)[1].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx")
				So(items[0].Current.State, ShouldEqual, "edition-confirmed")
				So(items[0].Next.Edition, ShouldEqual, "time-series")
				So(items[0].Next.Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(items[0].Next.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[0].Next.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(items[0].Next.Links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				So((*items[0].Next.Distributions)[0].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv")
				So((*items[0].Next.Distributions)[1].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx")
				So(items[0].Next.State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update links and distributions do not need rewriting", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
							},
						},
						Distributions: &[]models.Distribution{
							{
								Title:       "Distribution 1",
								Format:      "CSV",
								MediaType:   "text/csv",
								DownloadURL: "http://localhost:23600/downloads/files/datasets/cpih01/editions/time-series/versions/1.csv",
								ByteSize:    10000,
							},
							{
								Title:       "Distribution 2",
								Format:      "XLSX",
								MediaType:   "text/xlsx",
								DownloadURL: "http://localhost:23600/downloads/files/datasets/cpih01/editions/time-series/versions/1.xlsx",
								ByteSize:    20000,
							},
						},
						State: "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
							},
						},
						Distributions: &[]models.Distribution{
							{
								Title:       "Distribution 1",
								Format:      "CSV",
								MediaType:   "text/csv",
								DownloadURL: "http://localhost:23600/downloads/files/datasets/cpih01/editions/time-series/versions/1.csv",
								ByteSize:    10000,
							},
							{
								Title:       "Distribution 2",
								Format:      "XLSX",
								MediaType:   "text/xlsx",
								DownloadURL: "http://localhost:23600/downloads/files/datasets/cpih01/editions/time-series/versions/1.xlsx",
								ByteSize:    20000,
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithAuth(ctx, results, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, results[0].ID)
				So(items[0].Current.Edition, ShouldEqual, results[0].Current.Edition)
				So(items[0].Current.Links.Dataset.HRef, ShouldEqual, results[0].Current.Links.Dataset.HRef)
				So(items[0].Current.Links.LatestVersion.HRef, ShouldEqual, results[0].Current.Links.LatestVersion.HRef)
				So(items[0].Current.Links.Self.HRef, ShouldEqual, results[0].Current.Links.Self.HRef)
				So(items[0].Current.Links.Versions.HRef, ShouldEqual, results[0].Current.Links.Versions.HRef)
				So((*items[0].Current.Distributions)[0].DownloadURL, ShouldEqual, (*results[0].Current.Distributions)[0].DownloadURL)
				So((*items[0].Current.Distributions)[1].DownloadURL, ShouldEqual, (*results[0].Current.Distributions)[1].DownloadURL)
				So(items[0].Current.State, ShouldEqual, results[0].Current.State)
				So(items[0].Next.Edition, ShouldEqual, results[0].Next.Edition)
				So(items[0].Next.Links.Dataset.HRef, ShouldEqual, results[0].Next.Links.Dataset.HRef)
				So(items[0].Next.Links.LatestVersion.HRef, ShouldEqual, results[0].Next.Links.LatestVersion.HRef)
				So(items[0].Next.Links.Self.HRef, ShouldEqual, results[0].Next.Links.Self.HRef)
				So(items[0].Next.Links.Versions.HRef, ShouldEqual, results[0].Next.Links.Versions.HRef)
				So((*items[0].Next.Distributions)[0].DownloadURL, ShouldEqual, (*results[0].Next.Distributions)[0].DownloadURL)
				So((*items[0].Next.Distributions)[1].DownloadURL, ShouldEqual, (*results[0].Next.Distributions)[1].DownloadURL)
				So(items[0].Next.State, ShouldEqual, results[0].Next.State)
			})
		})

		Convey("When the edition updates are nil", func() {
			items, err := RewriteEditionsWithAuth(ctx, nil, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the edition updates should remain nil", func() {
				So(err, ShouldBeNil)
				So(items, ShouldBeNil)
			})
		})

		Convey("When the edition updates are empty", func() {
			items, err := RewriteEditionsWithAuth(ctx, []*models.EditionUpdate{}, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items, ShouldResemble, []*models.EditionUpdate{})
			})
		})

		Convey("When the edition update links are empty", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links:   &models.EditionUpdateLinks{},
						State:   "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links:   &models.EditionUpdateLinks{},
						State:   "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithAuth(ctx, results, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(items[0].Current.Links, ShouldResemble, &models.EditionUpdateLinks{})
				So(items[0].Current.State, ShouldEqual, "edition-confirmed")
				So(items[0].Next.Links, ShouldResemble, &models.EditionUpdateLinks{})
				So(items[0].Next.State, ShouldEqual, "edition-confirmed")
			})
		})
	})
}

func TestRewriteEditionsWithAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of edition updates", t, func() {
		Convey("When the 'current' edition update links are unable to be parsed", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithAuth(ctx, results, nil, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the 'next' edition update links are unable to be parsed", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Next: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithAuth(ctx, results, nil, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteEditionsWithoutAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of edition updates", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the edition update links and distributions need rewriting", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						Distributions: &[]models.Distribution{
							{
								Title:       "Distribution 1",
								Format:      "CSV",
								MediaType:   "text/csv",
								DownloadURL: "/cpih01/time-series/1/filename.csv",
								ByteSize:    10000,
							},
							{
								Title:       "Distribution 2",
								Format:      "XLSX",
								MediaType:   "text/xlsx",
								DownloadURL: "/cpih01/time-series/1/filename.xlsx",
								ByteSize:    20000,
							},
						},
						State: "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/2",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						Distributions: &[]models.Distribution{
							{
								Title:       "Distribution 1",
								Format:      "CSV",
								MediaType:   "text/csv",
								DownloadURL: "/cpih01/time-series/1/filename.csv",
								ByteSize:    10000,
							},
							{
								Title:       "Distribution 2",
								Format:      "XLSX",
								MediaType:   "text/xlsx",
								DownloadURL: "/cpih01/time-series/1/filename.xlsx",
								ByteSize:    20000,
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithoutAuth(ctx, results, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(items[0].Edition, ShouldEqual, "time-series")
				So(items[0].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(items[0].Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[0].Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(items[0].Links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				So((*items[0].Distributions)[0].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv")
				So((*items[0].Distributions)[1].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx")
				So(items[0].State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update links and distributions do not need rewriting", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
							},
						},
						Distributions: &[]models.Distribution{
							{
								Title:       "Distribution 1",
								Format:      "CSV",
								MediaType:   "text/csv",
								DownloadURL: "http://localhost:23600/downloads/files/datasets/cpih01/editions/time-series/versions/1.csv",
								ByteSize:    10000,
							},
							{
								Title:       "Distribution 2",
								Format:      "XLSX",
								MediaType:   "text/xlsx",
								DownloadURL: "http://localhost:23600/downloads/files/datasets/cpih01/editions/time-series/versions/1.xlsx",
								ByteSize:    20000,
							},
						},
						State: "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/2",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
							},
						},
						Distributions: &[]models.Distribution{
							{
								Title:       "Distribution 1",
								Format:      "CSV",
								MediaType:   "text/csv",
								DownloadURL: "http://localhost:23600/downloads/files/datasets/cpih01/editions/time-series/versions/1.csv",
								ByteSize:    10000,
							},
							{
								Title:       "Distribution 2",
								Format:      "XLSX",
								MediaType:   "text/xlsx",
								DownloadURL: "http://localhost:23600/downloads/files/datasets/cpih01/editions/time-series/versions/1.xlsx",
								ByteSize:    20000,
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithoutAuth(ctx, results, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, results[0].ID)
				So(items[0].Edition, ShouldEqual, results[0].Current.Edition)
				So(items[0].Links.Dataset.HRef, ShouldEqual, results[0].Current.Links.Dataset.HRef)
				So(items[0].Links.LatestVersion.HRef, ShouldEqual, results[0].Current.Links.LatestVersion.HRef)
				So(items[0].Links.Self.HRef, ShouldEqual, results[0].Current.Links.Self.HRef)
				So(items[0].Links.Versions.HRef, ShouldEqual, results[0].Current.Links.Versions.HRef)
				So((*items[0].Distributions)[0].DownloadURL, ShouldEqual, (*results[0].Current.Distributions)[0].DownloadURL)
				So((*items[0].Distributions)[1].DownloadURL, ShouldEqual, (*results[0].Current.Distributions)[1].DownloadURL)
				So(items[0].State, ShouldEqual, results[0].Current.State)
			})
		})

		Convey("When the edition updates are nil", func() {
			items, err := RewriteEditionsWithoutAuth(ctx, nil, datasetLinksBuilder, downloadServiceURL)

			Convey("Then an empty list should be returned", func() {
				So(err, ShouldBeNil)
				So(items, ShouldResemble, []*models.Edition{})
			})
		})

		Convey("When the edition updates are empty", func() {
			items, err := RewriteEditionsWithoutAuth(ctx, []*models.EditionUpdate{}, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items, ShouldResemble, []*models.Edition{})
			})
		})

		Convey("When the edition update links are empty", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links:   &models.EditionUpdateLinks{},
						State:   "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links:   &models.EditionUpdateLinks{},
						State:   "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithoutAuth(ctx, results, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(items[0].ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(items[0].Links, ShouldResemble, &models.EditionUpdateLinks{})
				So(items[0].State, ShouldEqual, "edition-confirmed")
			})
		})
	})
}

func TestRewriteEditionsWithoutAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of edition updates", t, func() {
		Convey("When the edition update links are unable to be parsed", func() {
			results := []*models.EditionUpdate{
				{
					ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
					Current: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
					Next: &models.Edition{
						Edition: "time-series",
						Links: &models.EditionUpdateLinks{
							Dataset: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01",
								ID:   "cpih01",
							},
							LatestVersion: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/2",
								ID:   "1",
							},
							Self: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							},
							Versions: &models.LinkObject{
								HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
							},
						},
						State: "edition-confirmed",
					},
				},
			}

			items, err := RewriteEditionsWithoutAuth(ctx, results, nil, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteEditionWithAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given an edition update", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the edition update links and distributions need rewriting", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Distribution 1",
							Format:      "CSV",
							MediaType:   "text/csv",
							DownloadURL: "/cpih01/time-series/1/filename.csv",
							ByteSize:    10000,
						},
						{
							Title:       "Distribution 2",
							Format:      "XLSX",
							MediaType:   "text/xlsx",
							DownloadURL: "/cpih01/time-series/1/filename.xlsx",
							ByteSize:    20000,
						},
					},
					State: "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/2",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Distribution 1",
							Format:      "CSV",
							MediaType:   "text/csv",
							DownloadURL: "/cpih01/time-series/1/filename.csv",
							ByteSize:    10000,
						},
						{
							Title:       "Distribution 2",
							Format:      "XLSX",
							MediaType:   "text/xlsx",
							DownloadURL: "/cpih01/time-series/1/filename.xlsx",
							ByteSize:    20000,
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithAuth(ctx, result, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(item.Current.Edition, ShouldEqual, "time-series")
				So(item.Current.Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(item.Current.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(item.Current.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(item.Current.Links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				So((*item.Current.Distributions)[0].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv")
				So((*item.Current.Distributions)[1].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx")
				So(item.Current.State, ShouldEqual, "edition-confirmed")
				So(item.Next.Edition, ShouldEqual, "time-series")
				So(item.Next.Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(item.Next.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/2")
				So(item.Next.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(item.Next.Links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				So((*item.Next.Distributions)[0].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv")
				So((*item.Next.Distributions)[1].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx")
				So(item.Next.State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update links and distributions do not need rewriting", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Distribution 1",
							Format:      "CSV",
							MediaType:   "text/csv",
							DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv",
							ByteSize:    10000,
						},
						{
							Title:       "Distribution 2",
							Format:      "XLSX",
							MediaType:   "text/xlsx",
							DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx",
							ByteSize:    20000,
						},
					},
					State: "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/2",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Distribution 1",
							Format:      "CSV",
							MediaType:   "text/csv",
							DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv",
							ByteSize:    10000,
						},
						{
							Title:       "Distribution 2",
							Format:      "XLSX",
							MediaType:   "text/xlsx",
							DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx",
							ByteSize:    20000,
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithAuth(ctx, result, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, result.ID)
				So(item.Current.Edition, ShouldEqual, result.Current.Edition)
				So(item.Current.Links.Dataset.HRef, ShouldEqual, result.Current.Links.Dataset.HRef)
				So(item.Current.Links.LatestVersion.HRef, ShouldEqual, result.Current.Links.LatestVersion.HRef)
				So(item.Current.Links.Self.HRef, ShouldEqual, result.Current.Links.Self.HRef)
				So(item.Current.Links.Versions.HRef, ShouldEqual, result.Current.Links.Versions.HRef)
				So((*item.Current.Distributions)[0].DownloadURL, ShouldEqual, (*result.Current.Distributions)[0].DownloadURL)
				So((*item.Current.Distributions)[1].DownloadURL, ShouldEqual, (*result.Current.Distributions)[1].DownloadURL)
				So(item.Current.State, ShouldEqual, result.Current.State)
				So(item.Next.Edition, ShouldEqual, result.Next.Edition)
				So(item.Next.Links.Dataset.HRef, ShouldEqual, result.Next.Links.Dataset.HRef)
				So(item.Next.Links.LatestVersion.HRef, ShouldEqual, result.Next.Links.LatestVersion.HRef)
				So(item.Next.Links.Self.HRef, ShouldEqual, result.Next.Links.Self.HRef)
				So(item.Next.Links.Versions.HRef, ShouldEqual, result.Next.Links.Versions.HRef)
				So((*item.Next.Distributions)[0].DownloadURL, ShouldEqual, (*result.Next.Distributions)[0].DownloadURL)
				So((*item.Next.Distributions)[1].DownloadURL, ShouldEqual, (*result.Next.Distributions)[1].DownloadURL)
				So(item.Next.State, ShouldEqual, result.Next.State)
			})
		})

		Convey("When the edition update links are empty", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links:   &models.EditionUpdateLinks{},
					State:   "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links:   &models.EditionUpdateLinks{},
					State:   "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithAuth(ctx, result, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(item.Current.Links, ShouldResemble, &models.EditionUpdateLinks{})
				So(item.Current.State, ShouldEqual, "edition-confirmed")
				So(item.Next.Links, ShouldResemble, &models.EditionUpdateLinks{})
				So(item.Next.State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update links are nil", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					State:   "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					State:   "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithAuth(ctx, result, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(item.Current.Links, ShouldBeNil)
				So(item.Current.State, ShouldEqual, "edition-confirmed")
				So(item.Next.Links, ShouldBeNil)
				So(item.Next.State, ShouldEqual, "edition-confirmed")
			})
		})
	})
}

func TestRewriteEditionWithAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given an edition update", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the 'current' edition update links are unable to be parsed", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithAuth(ctx, result, nil, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the 'next' edition update links are unable to be parsed", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Next: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithAuth(ctx, result, nil, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the edition update is nil", func() {
			item, err := RewriteEditionWithAuth(ctx, nil, datasetLinksBuilder, downloadServiceURL)

			Convey("Then an edition not found error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "edition not found")
			})
		})
	})
}

func TestRewriteEditionWithoutAuth_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given an edition update", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the edition update links and distributions need rewriting", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Distribution 1",
							Format:      "CSV",
							MediaType:   "text/csv",
							DownloadURL: "/cpih01/time-series/1/filename.csv",
							ByteSize:    10000,
						},
						{
							Title:       "Distribution 2",
							Format:      "XLSX",
							MediaType:   "text/xlsx",
							DownloadURL: "/cpih01/time-series/1/filename.xlsx",
							ByteSize:    20000,
						},
					},
					State: "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/2",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Distribution 1",
							Format:      "CSV",
							MediaType:   "text/csv",
							DownloadURL: "/cpih01/time-series/1/filename.csv",
							ByteSize:    10000,
						},
						{
							Title:       "Distribution 2",
							Format:      "XLSX",
							MediaType:   "text/xlsx",
							DownloadURL: "/cpih01/time-series/1/filename.xlsx",
							ByteSize:    20000,
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithoutAuth(ctx, result, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(item.Edition, ShouldEqual, "time-series")
				So(item.Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(item.Links.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(item.Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(item.Links.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
				So((*item.Distributions)[0].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv")
				So((*item.Distributions)[1].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx")
				So(item.State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update links and distributions do not need rewriting", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Distribution 1",
							Format:      "CSV",
							MediaType:   "text/csv",
							DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv",
							ByteSize:    10000,
						},
						{
							Title:       "Distribution 2",
							Format:      "XLSX",
							MediaType:   "text/xlsx",
							DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx",
							ByteSize:    20000,
						},
					},
					State: "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/2",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Distribution 1",
							Format:      "CSV",
							MediaType:   "text/csv",
							DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv",
							ByteSize:    10000,
						},
						{
							Title:       "Distribution 2",
							Format:      "XLSX",
							MediaType:   "text/xlsx",
							DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx",
							ByteSize:    20000,
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithoutAuth(ctx, result, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, result.ID)
				So(item.Edition, ShouldEqual, result.Current.Edition)
				So(item.Links.Dataset.HRef, ShouldEqual, result.Current.Links.Dataset.HRef)
				So(item.Links.LatestVersion.HRef, ShouldEqual, result.Current.Links.LatestVersion.HRef)
				So(item.Links.Self.HRef, ShouldEqual, result.Current.Links.Self.HRef)
				So(item.Links.Versions.HRef, ShouldEqual, result.Current.Links.Versions.HRef)
				So((*item.Distributions)[0].DownloadURL, ShouldEqual, (*result.Current.Distributions)[0].DownloadURL)
				So((*item.Distributions)[1].DownloadURL, ShouldEqual, (*result.Current.Distributions)[1].DownloadURL)
				So(item.State, ShouldEqual, result.Current.State)
			})
		})

		Convey("When the edition update links are empty", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links:   &models.EditionUpdateLinks{},
					State:   "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links:   &models.EditionUpdateLinks{},
					State:   "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithoutAuth(ctx, result, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(item.Links, ShouldResemble, &models.EditionUpdateLinks{})
				So(item.State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update links are nil", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					State:   "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					State:   "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithoutAuth(ctx, result, datasetLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
				So(item.ID, ShouldEqual, "66f7219d-6d53-402a-87b6-cb4014f7179f")
				So(item.Links, ShouldBeNil)
				So(item.State, ShouldEqual, "edition-confirmed")
			})
		})

		Convey("When the edition update is empty", func() {
			item, err := RewriteEditionWithoutAuth(ctx, &models.EditionUpdate{}, datasetLinksBuilder, downloadServiceURL)

			Convey("Then nothing should be returned", func() {
				So(err, ShouldBeNil)
				So(item, ShouldBeNil)
			})
		})

		Convey("When the edition update 'current' is nil", func() {
			item, err := RewriteEditionWithoutAuth(ctx, &models.EditionUpdate{
				Current: nil,
				Next:    &models.Edition{},
			}, datasetLinksBuilder, downloadServiceURL)

			Convey("Then nothing should be returned", func() {
				So(err, ShouldBeNil)
				So(item, ShouldBeNil)
			})
		})
	})
}

func TestRewriteEditionWithoutAuth_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given an edition update", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the edition update links are unable to be parsed", func() {
			result := &models.EditionUpdate{
				ID: "66f7219d-6d53-402a-87b6-cb4014f7179f",
				Current: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
				Next: &models.Edition{
					Edition: "time-series",
					Links: &models.EditionUpdateLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						LatestVersion: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/2",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
						},
						Versions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
						},
					},
					State: "edition-confirmed",
				},
			}

			item, err := RewriteEditionWithoutAuth(ctx, result, nil, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the edition update is nil", func() {
			item, err := RewriteEditionWithoutAuth(ctx, nil, datasetLinksBuilder, downloadServiceURL)

			Convey("Then an edition not found error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(item, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "edition not found")
			})
		})
	})
}

func TestRewriteEditionLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of edition update links", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the edition update links need rewriting", func() {
			editionUpdateLinks := &models.EditionUpdateLinks{
				Dataset: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01",
					ID:   "cpih01",
				},
				LatestVersion: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				Self: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
				},
				Versions: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions",
				},
			}

			err := RewriteEditionLinks(ctx, editionUpdateLinks, datasetLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(editionUpdateLinks.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(editionUpdateLinks.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(editionUpdateLinks.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(editionUpdateLinks.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
			})
		})

		Convey("When the edition update links do not need rewriting", func() {
			editionUpdateLinks := &models.EditionUpdateLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01",
					ID:   "cpih01",
				},
				LatestVersion: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
				},
				Versions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions",
				},
			}

			err := RewriteEditionLinks(ctx, editionUpdateLinks, datasetLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(editionUpdateLinks.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(editionUpdateLinks.LatestVersion.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(editionUpdateLinks.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(editionUpdateLinks.Versions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions")
			})
		})

		Convey("When the edition update links are empty", func() {
			editionUpdateLinks := &models.EditionUpdateLinks{}

			err := RewriteEditionLinks(ctx, editionUpdateLinks, datasetLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(editionUpdateLinks, ShouldResemble, &models.EditionUpdateLinks{})
			})
		})

		Convey("When the edition update links are nil", func() {
			err := RewriteEditionLinks(ctx, nil, datasetLinksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteEditionLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of edition update links", t, func() {
		Convey("When the edition update links are unable to be parsed", func() {
			editionUpdateLinks := &models.EditionUpdateLinks{
				Dataset: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01",
					ID:   "cpih01",
				},
				LatestVersion: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				Self: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
				},
				Versions: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions",
				},
			}

			err := RewriteEditionLinks(ctx, editionUpdateLinks, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteMetadataLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of metadata links", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the metadata links need rewriting", func() {
			metadataLinks := &models.MetadataLinks{
				AccessRights: &models.LinkObject{
					HRef: "https://oldhost:1000/accessrights",
				},
				Self: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/metadata",
				},
				Version: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				WebsiteVersion: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteMetadataLinks(ctx, metadataLinks, datasetLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(metadataLinks.AccessRights.HRef, ShouldEqual, "https://oldhost:1000/accessrights")
				So(metadataLinks.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/metadata")
				So(metadataLinks.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(metadataLinks.Version.ID, ShouldEqual, "1")
				So(metadataLinks.WebsiteVersion.HRef, ShouldEqual, "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the metadata links do not need rewriting", func() {
			metadataLinks := &models.MetadataLinks{
				AccessRights: &models.LinkObject{
					HRef: "http://localhost:22000/accessrights",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/metadata",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				WebsiteVersion: &models.LinkObject{
					HRef: "http://localhost:20000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteMetadataLinks(ctx, metadataLinks, datasetLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(metadataLinks.AccessRights.HRef, ShouldEqual, "http://localhost:22000/accessrights")
				So(metadataLinks.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/metadata")
				So(metadataLinks.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(metadataLinks.Version.ID, ShouldEqual, "1")
				So(metadataLinks.WebsiteVersion.HRef, ShouldEqual, "http://localhost:20000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the metadata links are empty", func() {
			metadataLinks := &models.MetadataLinks{}

			err := RewriteMetadataLinks(ctx, metadataLinks, datasetLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(metadataLinks, ShouldResemble, &models.MetadataLinks{})
			})
		})

		Convey("When the metadata links are nil", func() {
			err := RewriteMetadataLinks(ctx, nil, datasetLinksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When the metadata links are missing", func() {
			err := RewriteMetadataLinks(ctx, &models.MetadataLinks{}, datasetLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteMetadataLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of metadata links", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the Self link is unable to be parsed", func() {
			metadataLinks := &models.MetadataLinks{
				Self: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/metadata",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				WebsiteVersion: &models.LinkObject{
					HRef: "http://localhost:20000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteMetadataLinks(ctx, metadataLinks, datasetLinksBuilder)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the Version link is unable to be parsed", func() {
			metadataLinks := &models.MetadataLinks{
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/metadata",
				},
				Version: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
				WebsiteVersion: &models.LinkObject{
					HRef: "http://localhost:20000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteMetadataLinks(ctx, metadataLinks, datasetLinksBuilder)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteVersions_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of versions", t, func() {
		codeListLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, codeListAPIURL)
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)

		Convey("When the version, dimension, download and distribution links need rewriting", func() {
			results := []models.Version{
				{
					ID:        "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   53,
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						Edition: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/53",
						},
					},
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
					Downloads: &models.DownloadList{
						CSV: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.csv",
							Size: "15000",
						},
						CSVW: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json",
							Size: "30000",
						},
						TXT: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.txt",
							Size: "45000",
						},
						XLS: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.xls",
							Size: "60000",
						},
						XLSX: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx",
							Size: "75000",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Distribution 1",
							Format:      "CSV",
							MediaType:   "text/csv",
							DownloadURL: "/cpih01/time-series/1/filename.csv",
							ByteSize:    10000,
						},
						{
							Title:       "Distribution 2",
							Format:      "XLSX",
							MediaType:   "text/xlsx",
							DownloadURL: "/cpih01/time-series/1/filename.xlsx",
							ByteSize:    20000,
						},
					},
				},
				{
					ID:        "74e4d2da-8fd6-4bb6-b4a2-b5cd573fb42b",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   52,
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						Edition: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/52",
						},
					},
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
					Downloads: &models.DownloadList{
						CSV: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.csv",
							Size: "15000",
						},
						CSVW: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json",
							Size: "30000",
						},
						TXT: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.txt",
							Size: "45000",
						},
						XLS: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.xls",
							Size: "60000",
						},
						XLSX: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx",
							Size: "75000",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Distribution 1",
							Format:      "CSV",
							MediaType:   "text/csv",
							DownloadURL: "/cpih01/time-series/1/filename.csv",
							ByteSize:    10000,
						},
						{
							Title:       "Distribution 2",
							Format:      "XLSX",
							MediaType:   "text/xlsx",
							DownloadURL: "/cpih01/time-series/1/filename.xlsx",
							ByteSize:    20000,
						},
					},
				},
			}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder, downloadServiceURL)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)

				So(items[0].ID, ShouldEqual, "cf4b2196-3548-4bd5-8288-92fe4ca06327")
				So(items[0].DatasetID, ShouldEqual, "cpih01")
				So(items[0].Edition, ShouldEqual, "time-series")
				So(items[0].Version, ShouldEqual, 53)
				So(items[0].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(items[0].Links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(items[0].Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/53")
				So(items[0].Dimensions[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				So(items[0].Dimensions[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				So(items[0].Dimensions[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[0].Dimensions[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				So(items[0].Dimensions[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				So(items[0].Dimensions[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[0].Downloads.CSV.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv")
				So(items[0].Downloads.CSV.Size, ShouldEqual, "15000")
				So(items[0].Downloads.CSVW.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json")
				So(items[0].Downloads.CSVW.Size, ShouldEqual, "30000")
				So(items[0].Downloads.TXT.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.txt")
				So(items[0].Downloads.TXT.Size, ShouldEqual, "45000")
				So(items[0].Downloads.XLS.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xls")
				So(items[0].Downloads.XLS.Size, ShouldEqual, "60000")
				So(items[0].Downloads.XLSX.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx")
				So(items[0].Downloads.XLSX.Size, ShouldEqual, "75000")
				So((*items[0].Distributions)[0].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv")
				So((*items[0].Distributions)[1].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx")

				So(items[1].ID, ShouldEqual, "74e4d2da-8fd6-4bb6-b4a2-b5cd573fb42b")
				So(items[1].DatasetID, ShouldEqual, "cpih01")
				So(items[1].Edition, ShouldEqual, "time-series")
				So(items[1].Version, ShouldEqual, 52)
				So(items[1].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(items[1].Links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(items[1].Links.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/52")
				So(items[1].Dimensions[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				So(items[1].Dimensions[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				So(items[1].Dimensions[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[1].Dimensions[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				So(items[1].Dimensions[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				So(items[1].Dimensions[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(items[1].Downloads.CSV.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv")
				So(items[1].Downloads.CSV.Size, ShouldEqual, "15000")
				So(items[1].Downloads.CSVW.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json")
				So(items[1].Downloads.CSVW.Size, ShouldEqual, "30000")
				So(items[1].Downloads.TXT.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.txt")
				So(items[1].Downloads.TXT.Size, ShouldEqual, "45000")
				So(items[1].Downloads.XLS.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xls")
				So(items[1].Downloads.XLS.Size, ShouldEqual, "60000")
				So(items[1].Downloads.XLSX.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx")
				So(items[1].Downloads.XLSX.Size, ShouldEqual, "75000")
				So((*items[1].Distributions)[0].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv")
				So((*items[1].Distributions)[1].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx")
			})
		})

		Convey("When the version, dimension, download and distribution links do not need rewriting", func() {
			results := []models.Version{
				{
					ID:        "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   53,
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/53",
						},
					},
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
					Downloads: &models.DownloadList{
						CSV: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv",
							Size: "15000",
						},
						CSVW: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json",
							Size: "30000",
						},
						TXT: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.txt",
							Size: "45000",
						},
						XLS: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xls",
							Size: "60000",
						},
						XLSX: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx",
							Size: "75000",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Distribution 1",
							Format:      "CSV",
							MediaType:   "text/csv",
							DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv",
							ByteSize:    10000,
						},
						{
							Title:       "Distribution 2",
							Format:      "XLSX",
							MediaType:   "text/xlsx",
							DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx",
							ByteSize:    20000,
						},
					},
				},
				{
					ID:        "74e4d2da-8fd6-4bb6-b4a2-b5cd573fb42b",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   52,
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/52",
						},
					},
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
					Downloads: &models.DownloadList{
						CSV: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv",
							Size: "15000",
						},
						CSVW: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json",
							Size: "30000",
						},
						TXT: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.txt",
							Size: "45000",
						},
						XLS: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xls",
							Size: "60000",
						},
						XLSX: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx",
							Size: "75000",
						},
					},
					Distributions: &[]models.Distribution{
						{
							Title:       "Distribution 1",
							Format:      "CSV",
							MediaType:   "text/csv",
							DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv",
							ByteSize:    10000,
						},
						{
							Title:       "Distribution 2",
							Format:      "XLSX",
							MediaType:   "text/xlsx",
							DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx",
							ByteSize:    20000,
						},
					},
				},
			}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)

				So(items[0].ID, ShouldEqual, results[0].ID)
				So(items[0].DatasetID, ShouldEqual, results[0].DatasetID)
				So(items[0].Edition, ShouldEqual, results[0].Edition)
				So(items[0].Version, ShouldEqual, results[0].Version)
				So(items[0].Links.Dataset.HRef, ShouldEqual, results[0].Links.Dataset.HRef)
				So(items[0].Links.Edition.HRef, ShouldEqual, results[0].Links.Edition.HRef)
				So(items[0].Links.Self.HRef, ShouldEqual, results[0].Links.Self.HRef)
				So(items[0].Dimensions[0].Links.CodeList.HRef, ShouldEqual, results[0].Dimensions[0].Links.CodeList.HRef)
				So(items[0].Dimensions[0].Links.Options.HRef, ShouldEqual, results[0].Dimensions[0].Links.Options.HRef)
				So(items[0].Dimensions[0].Links.Version.HRef, ShouldEqual, results[0].Dimensions[0].Links.Version.HRef)
				So(items[0].Dimensions[1].Links.CodeList.HRef, ShouldEqual, results[0].Dimensions[1].Links.CodeList.HRef)
				So(items[0].Dimensions[1].Links.Options.HRef, ShouldEqual, results[0].Dimensions[1].Links.Options.HRef)
				So(items[0].Dimensions[1].Links.Version.HRef, ShouldEqual, results[0].Dimensions[1].Links.Version.HRef)
				So(items[0].Downloads.CSV.HRef, ShouldEqual, results[0].Downloads.CSV.HRef)
				So(items[0].Downloads.CSV.Size, ShouldEqual, results[0].Downloads.CSV.Size)
				So(items[0].Downloads.CSVW.HRef, ShouldEqual, results[0].Downloads.CSVW.HRef)
				So(items[0].Downloads.CSVW.Size, ShouldEqual, results[0].Downloads.CSVW.Size)
				So(items[0].Downloads.TXT.HRef, ShouldEqual, results[0].Downloads.TXT.HRef)
				So(items[0].Downloads.TXT.Size, ShouldEqual, results[0].Downloads.TXT.Size)
				So(items[0].Downloads.XLS.HRef, ShouldEqual, results[0].Downloads.XLS.HRef)
				So(items[0].Downloads.XLS.Size, ShouldEqual, results[0].Downloads.XLS.Size)
				So(items[0].Downloads.XLSX.HRef, ShouldEqual, results[0].Downloads.XLSX.HRef)
				So(items[0].Downloads.XLSX.Size, ShouldEqual, results[0].Downloads.XLSX.Size)
				So((*items[0].Distributions)[0].DownloadURL, ShouldEqual, (*results[0].Distributions)[0].DownloadURL)
				So((*items[0].Distributions)[1].DownloadURL, ShouldEqual, (*results[0].Distributions)[1].DownloadURL)

				So(items[1].ID, ShouldEqual, results[1].ID)
				So(items[1].DatasetID, ShouldEqual, results[1].DatasetID)
				So(items[1].Edition, ShouldEqual, results[1].Edition)
				So(items[1].Version, ShouldEqual, results[1].Version)
				So(items[1].Links.Dataset.HRef, ShouldEqual, results[1].Links.Dataset.HRef)
				So(items[1].Links.Edition.HRef, ShouldEqual, results[1].Links.Edition.HRef)
				So(items[1].Links.Self.HRef, ShouldEqual, results[1].Links.Self.HRef)
				So(items[1].Dimensions[0].Links.CodeList.HRef, ShouldEqual, results[1].Dimensions[0].Links.CodeList.HRef)
				So(items[1].Dimensions[0].Links.Options.HRef, ShouldEqual, results[1].Dimensions[0].Links.Options.HRef)
				So(items[1].Dimensions[0].Links.Version.HRef, ShouldEqual, results[1].Dimensions[0].Links.Version.HRef)
				So(items[1].Dimensions[1].Links.CodeList.HRef, ShouldEqual, results[1].Dimensions[1].Links.CodeList.HRef)
				So(items[1].Dimensions[1].Links.Options.HRef, ShouldEqual, results[1].Dimensions[1].Links.Options.HRef)
				So(items[1].Dimensions[1].Links.Version.HRef, ShouldEqual, results[1].Dimensions[1].Links.Version.HRef)
				So(items[1].Downloads.CSV.HRef, ShouldEqual, results[1].Downloads.CSV.HRef)
				So(items[1].Downloads.CSV.Size, ShouldEqual, results[1].Downloads.CSV.Size)
				So(items[1].Downloads.CSVW.HRef, ShouldEqual, results[1].Downloads.CSVW.HRef)
				So(items[1].Downloads.CSVW.Size, ShouldEqual, results[1].Downloads.CSVW.Size)
				So(items[1].Downloads.TXT.HRef, ShouldEqual, results[1].Downloads.TXT.HRef)
				So(items[1].Downloads.TXT.Size, ShouldEqual, results[1].Downloads.TXT.Size)
				So(items[1].Downloads.XLS.HRef, ShouldEqual, results[1].Downloads.XLS.HRef)
				So(items[1].Downloads.XLS.Size, ShouldEqual, results[1].Downloads.XLS.Size)
				So(items[1].Downloads.XLSX.HRef, ShouldEqual, results[1].Downloads.XLSX.HRef)
				So(items[1].Downloads.XLSX.Size, ShouldEqual, results[1].Downloads.XLSX.Size)
				So((*items[1].Distributions)[0].DownloadURL, ShouldEqual, (*results[1].Distributions)[0].DownloadURL)
				So((*items[1].Distributions)[1].DownloadURL, ShouldEqual, (*results[1].Distributions)[1].DownloadURL)
			})
		})

		Convey("When the version, dimension, download and distribution links are empty", func() {
			results := []models.Version{
				{
					ID:            "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID:     "cpih01",
					Edition:       "time-series",
					Version:       53,
					Links:         &models.VersionLinks{},
					Dimensions:    []models.Dimension{},
					Downloads:     &models.DownloadList{},
					Distributions: &[]models.Distribution{},
				},
				{
					ID:            "74e4d2da-8fd6-4bb6-b4a2-b5cd573fb42b",
					DatasetID:     "cpih01",
					Edition:       "time-series",
					Version:       52,
					Links:         &models.VersionLinks{},
					Dimensions:    []models.Dimension{},
					Downloads:     &models.DownloadList{},
					Distributions: &[]models.Distribution{},
				},
			}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)

				So(items[0].ID, ShouldEqual, results[0].ID)
				So(items[0].DatasetID, ShouldEqual, results[0].DatasetID)
				So(items[0].Edition, ShouldEqual, results[0].Edition)
				So(items[0].Version, ShouldEqual, results[0].Version)
				So(items[0].Links, ShouldResemble, &models.VersionLinks{})
				So(items[0].Dimensions, ShouldResemble, []models.Dimension{})
				So(items[0].Downloads, ShouldResemble, &models.DownloadList{})
				So(items[0].Distributions, ShouldResemble, &[]models.Distribution{})

				So(items[1].ID, ShouldEqual, results[1].ID)
				So(items[1].DatasetID, ShouldEqual, results[1].DatasetID)
				So(items[1].Edition, ShouldEqual, results[1].Edition)
				So(items[1].Version, ShouldEqual, results[1].Version)
				So(items[1].Links, ShouldResemble, &models.VersionLinks{})
				So(items[1].Dimensions, ShouldResemble, []models.Dimension{})
				So(items[1].Downloads, ShouldResemble, &models.DownloadList{})
				So(items[1].Distributions, ShouldResemble, &[]models.Distribution{})
			})
		})

		Convey("When the version, dimension, download and distribution links are nil", func() {
			results := []models.Version{
				{
					ID:            "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID:     "cpih01",
					Edition:       "time-series",
					Version:       53,
					Links:         nil,
					Dimensions:    nil,
					Downloads:     nil,
					Distributions: nil,
				},
				{
					ID:            "74e4d2da-8fd6-4bb6-b4a2-b5cd573fb42b",
					DatasetID:     "cpih01",
					Edition:       "time-series",
					Version:       52,
					Links:         nil,
					Dimensions:    nil,
					Downloads:     nil,
					Distributions: nil,
				},
			}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)

				So(items[0].ID, ShouldEqual, results[0].ID)
				So(items[0].DatasetID, ShouldEqual, results[0].DatasetID)
				So(items[0].Edition, ShouldEqual, results[0].Edition)
				So(items[0].Version, ShouldEqual, results[0].Version)
				So(items[0].Links, ShouldBeNil)
				So(items[0].Dimensions, ShouldBeNil)
				So(items[0].Downloads, ShouldBeNil)
				So(items[0].Distributions, ShouldBeNil)

				So(items[1].ID, ShouldEqual, results[1].ID)
				So(items[1].DatasetID, ShouldEqual, results[1].DatasetID)
				So(items[1].Edition, ShouldEqual, results[1].Edition)
				So(items[1].Version, ShouldEqual, results[1].Version)
				So(items[1].Links, ShouldBeNil)
				So(items[1].Dimensions, ShouldBeNil)
				So(items[1].Downloads, ShouldBeNil)
				So(items[1].Distributions, ShouldBeNil)
			})
		})

		Convey("When the versions are empty", func() {
			results := []models.Version{}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder, downloadServiceURL)

			Convey("Then the versions should remain empty", func() {
				So(err, ShouldBeNil)
				So(items, ShouldBeEmpty)
			})
		})
	})
}

func TestRewriteVersions_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of versions", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		codeListLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, codeListAPIURL)
		Convey("When the version links are unable to be parsed", func() {
			results := []models.Version{
				{
					ID:        "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   53,
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						Edition: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/53",
						},
					},
				},
				{
					ID:        "74e4d2da-8fd6-4bb6-b4a2-b5cd573fb42b",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   52,
					Links: &models.VersionLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						Edition: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/52",
						},
					},
				},
			}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder, downloadServiceURL)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the dimension links are unable to be parsed", func() {
			results := []models.Version{
				{
					ID:        "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   53,
					Dimensions: []models.Dimension{
						{
							HRef:  "://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:    "cpih1dim1aggid",
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
								},
								Version: models.LinkObject{
									HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
					},
				},
			}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder, downloadServiceURL)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the download links are unable to be parsed", func() {
			results := []models.Version{
				{
					ID:        "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   53,
					Downloads: &models.DownloadList{
						CSV: &models.DownloadObject{
							HRef: "://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.csv",
						},
					},
				},
			}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder, downloadServiceURL)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the distribution links are unable to be parsed", func() {
			results := []models.Version{
				{
					ID:        "cf4b2196-3548-4bd5-8288-92fe4ca06327",
					DatasetID: "cpih01",
					Edition:   "time-series",
					Version:   53,
					Distributions: &[]models.Distribution{
						{
							DownloadURL: "://oldhost:1000/downloads/files/cpih01/time-series/1/filename.csv",
						},
					},
				},
			}

			items, err := RewriteVersions(ctx, results, datasetLinksBuilder, codeListLinksBuilder, downloadServiceURL)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(items, ShouldBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteVersionLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of version links", t, func() {
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the version links need rewriting", func() {
			versionLinks := &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01",
					ID:   "cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
					ID:   "time-series",
				},
				Self: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
				Spatial: &models.LinkObject{
					HRef: "https://oldhost:1000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
			}

			err := RewriteVersionLinks(ctx, versionLinks, datasetLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(versionLinks.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(versionLinks.Dimensions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				So(versionLinks.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(versionLinks.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(versionLinks.Spatial.HRef, ShouldEqual, "https://oldhost:1000/spatial")
				So(versionLinks.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(versionLinks.Version.ID, ShouldEqual, "1")
			})
		})

		Convey("When the version links do not need rewriting", func() {
			versionLinks := &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01",
					ID:   "cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
					ID:   "time-series",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
				Spatial: &models.LinkObject{
					HRef: "http://oldhost:1000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
			}

			err := RewriteVersionLinks(ctx, versionLinks, datasetLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(versionLinks.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(versionLinks.Dimensions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				So(versionLinks.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(versionLinks.Self.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(versionLinks.Spatial.HRef, ShouldEqual, "http://oldhost:1000/spatial")
				So(versionLinks.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(versionLinks.Version.ID, ShouldEqual, "1")
			})
		})

		Convey("When the version links are empty", func() {
			versionLinks := &models.VersionLinks{}

			err := RewriteVersionLinks(ctx, versionLinks, datasetLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(versionLinks, ShouldResemble, &models.VersionLinks{})
			})
		})

		Convey("When the version links are nil", func() {
			err := RewriteVersionLinks(ctx, nil, datasetLinksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteVersionLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of version links", t, func() {
		Convey("When the version links are unable to be parsed", func() {
			versionLinks := &models.VersionLinks{
				Dataset: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01",
					ID:   "cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
					ID:   "time-series",
				},
				Self: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
				Spatial: &models.LinkObject{
					HRef: "://oldhost:1000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
					ID:   "1",
				},
			}

			err := RewriteVersionLinks(ctx, versionLinks, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteInstances_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of instances", t, func() {
		importLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, importAPIURL)
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		codeListLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, codeListAPIURL)

		Convey("When the instance, dimension and download links need rewriting", func() {
			results := []*models.Instance{
				{
					CollectionID: "cantabularflexibledefault-1",
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
					Downloads: &models.DownloadList{
						CSV: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.csv",
							Size: "15000",
						},
						CSVW: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json",
							Size: "30000",
						},
						TXT: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.txt",
							Size: "45000",
						},
						XLS: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.xls",
							Size: "60000",
						},
						XLSX: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx",
							Size: "75000",
						},
					},
					Edition:    "2021",
					InstanceID: "1",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cantabular-flexible-default",
							ID:   "cantabular-flexible-default",
						},
						Dimensions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cantabular-flexible-default/editions/2021",
							ID:   "2021",
						},
						Job: &models.LinkObject{
							HRef: "https://oldhost:1000/jobs/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/instances/1",
						},
						Spatial: &models.LinkObject{
							HRef: "http://oldhost:1000/spatial/1",
						},
						Version: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cantabular-flexible-default/editions/2021/versions/1",
							ID:   "1",
						},
					},
				},
				{
					CollectionID: "cpihtest-1",
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "https://oldhost:1000/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
					Downloads: &models.DownloadList{
						CSV: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.csv",
							Size: "15000",
						},
						CSVW: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json",
							Size: "30000",
						},
						TXT: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.txt",
							Size: "45000",
						},
						XLS: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.xls",
							Size: "60000",
						},
						XLSX: &models.DownloadObject{
							HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx",
							Size: "75000",
						},
					},
					Edition:    "time-series",
					InstanceID: "2",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						Dimensions: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Job: &models.LinkObject{
							HRef: "https://oldhost:1000/jobs/2",
							ID:   "2",
						},
						Self: &models.LinkObject{
							HRef: "https://oldhost:1000/instances/2",
						},
						Spatial: &models.LinkObject{
							HRef: "http://oldhost:1000/spatial/2",
						},
						Version: &models.LinkObject{
							HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
					},
				},
			}

			err := RewriteInstances(ctx, results, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder, downloadServiceURL)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)

				So(results[0].CollectionID, ShouldEqual, "cantabularflexibledefault-1")
				So(results[0].Dimensions[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				So(results[0].Dimensions[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				So(results[0].Dimensions[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[0].Dimensions[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				So(results[0].Dimensions[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				So(results[0].Dimensions[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[0].Downloads.CSV.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv")
				So(results[0].Downloads.CSV.Size, ShouldEqual, "15000")
				So(results[0].Downloads.CSVW.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json")
				So(results[0].Downloads.CSVW.Size, ShouldEqual, "30000")
				So(results[0].Downloads.TXT.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.txt")
				So(results[0].Downloads.TXT.Size, ShouldEqual, "45000")
				So(results[0].Downloads.XLS.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xls")
				So(results[0].Downloads.XLS.Size, ShouldEqual, "60000")
				So(results[0].Downloads.XLSX.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx")
				So(results[0].Downloads.XLSX.Size, ShouldEqual, "75000")
				So(results[0].Edition, ShouldEqual, "2021")
				So(results[0].InstanceID, ShouldEqual, "1")
				So(results[0].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default")
				So(results[0].Links.Dimensions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				So(results[0].Links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021")
				So(results[0].Links.Job.HRef, ShouldEqual, "http://localhost:21800/jobs/1")
				So(results[0].Links.Self.HRef, ShouldEqual, "http://localhost:22000/instances/1")
				So(results[0].Links.Spatial.HRef, ShouldEqual, "http://oldhost:1000/spatial/1")
				So(results[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021/versions/1")
				So(results[1].CollectionID, ShouldEqual, "cpihtest-1")
				So(results[1].Dimensions[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				So(results[1].Dimensions[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				So(results[1].Dimensions[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[1].Dimensions[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				So(results[1].Dimensions[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				So(results[1].Dimensions[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[1].Downloads.CSV.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv")
				So(results[1].Downloads.CSV.Size, ShouldEqual, "15000")
				So(results[1].Downloads.CSVW.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json")
				So(results[1].Downloads.CSVW.Size, ShouldEqual, "30000")
				So(results[1].Downloads.TXT.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.txt")
				So(results[1].Downloads.TXT.Size, ShouldEqual, "45000")
				So(results[1].Downloads.XLS.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xls")
				So(results[1].Downloads.XLS.Size, ShouldEqual, "60000")
				So(results[1].Downloads.XLSX.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx")
				So(results[1].Downloads.XLSX.Size, ShouldEqual, "75000")
				So(results[1].Edition, ShouldEqual, "time-series")
				So(results[1].InstanceID, ShouldEqual, "2")
				So(results[1].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(results[1].Links.Dimensions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				So(results[1].Links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(results[1].Links.Job.HRef, ShouldEqual, "http://localhost:21800/jobs/2")
				So(results[1].Links.Self.HRef, ShouldEqual, "http://localhost:22000/instances/2")
				So(results[1].Links.Spatial.HRef, ShouldEqual, "http://oldhost:1000/spatial/2")
				So(results[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the instance, dimension and download links do not need rewriting", func() {
			results := []*models.Instance{
				{
					CollectionID: "cantabularflexibledefault-1",
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
					Downloads: &models.DownloadList{
						CSV: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv",
							Size: "15000",
						},
						CSVW: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json",
							Size: "30000",
						},
						TXT: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.txt",
							Size: "45000",
						},
						XLS: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xls",
							Size: "60000",
						},
						XLSX: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx",
							Size: "75000",
						},
					},
					Edition:    "2021",
					InstanceID: "1",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cantabular-flexible-default",
							ID:   "cantabular-flexible-default",
						},
						Dimensions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021",
							ID:   "2021",
						},
						Job: &models.LinkObject{
							HRef: "http://localhost:21800/jobs/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/instances/1",
						},
						Spatial: &models.LinkObject{
							HRef: "http://oldhost:1000/spatial/1",
						},
						Version: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021/versions/1",
							ID:   "1",
						},
					},
				},
				{
					CollectionID: "cpihtest-1",
					Dimensions: []models.Dimension{
						{
							Label: "Aggregate",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/cpih1dim1aggid",
									ID:   "cpih1dim1aggid",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options",
									ID:   "aggregate",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "aggregate",
						},
						{
							Label: "Geography",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{
									HRef: "http://localhost:22400/code-lists/uk-only",
									ID:   "uk-only",
								},
								Options: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options",
									ID:   "geography",
								},
								Version: models.LinkObject{
									HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
								},
							},
							Name: "geography",
						},
					},
					Downloads: &models.DownloadList{
						CSV: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv",
							Size: "15000",
						},
						CSVW: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json",
							Size: "30000",
						},
						TXT: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.txt",
							Size: "45000",
						},
						XLS: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xls",
							Size: "60000",
						},
						XLSX: &models.DownloadObject{
							HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx",
							Size: "75000",
						},
					},
					Edition:    "time-series",
					InstanceID: "2",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01",
							ID:   "cpih01",
						},
						Dimensions: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Job: &models.LinkObject{
							HRef: "http://localhost:21800/jobs/2",
							ID:   "2",
						},
						Self: &models.LinkObject{
							HRef: "http://localhost:22000/instances/2",
						},
						Spatial: &models.LinkObject{
							HRef: "http://oldhost:1000/spatial/2",
						},
						Version: &models.LinkObject{
							HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
					},
				},
			}

			err := RewriteInstances(ctx, results, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)

				So(results[0].CollectionID, ShouldEqual, "cantabularflexibledefault-1")
				So(results[0].Dimensions[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				So(results[0].Dimensions[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				So(results[0].Dimensions[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[0].Dimensions[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				So(results[0].Dimensions[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				So(results[0].Dimensions[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[0].Downloads.CSV.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv")
				So(results[0].Downloads.CSV.Size, ShouldEqual, "15000")
				So(results[0].Downloads.CSVW.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json")
				So(results[0].Downloads.CSVW.Size, ShouldEqual, "30000")
				So(results[0].Downloads.TXT.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.txt")
				So(results[0].Downloads.TXT.Size, ShouldEqual, "45000")
				So(results[0].Downloads.XLS.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xls")
				So(results[0].Downloads.XLS.Size, ShouldEqual, "60000")
				So(results[0].Downloads.XLSX.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx")
				So(results[0].Downloads.XLSX.Size, ShouldEqual, "75000")
				So(results[0].Edition, ShouldEqual, "2021")
				So(results[0].InstanceID, ShouldEqual, "1")
				So(results[0].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default")
				So(results[0].Links.Dimensions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				So(results[0].Links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021")
				So(results[0].Links.Job.HRef, ShouldEqual, "http://localhost:21800/jobs/1")
				So(results[0].Links.Self.HRef, ShouldEqual, "http://localhost:22000/instances/1")
				So(results[0].Links.Spatial.HRef, ShouldEqual, "http://oldhost:1000/spatial/1")
				So(results[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cantabular-flexible-default/editions/2021/versions/1")

				So(results[1].CollectionID, ShouldEqual, "cpihtest-1")
				So(results[1].Dimensions[0].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/cpih1dim1aggid")
				So(results[1].Dimensions[0].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/aggregate/options")
				So(results[1].Dimensions[0].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[1].Dimensions[1].Links.CodeList.HRef, ShouldEqual, "http://localhost:22400/code-lists/uk-only")
				So(results[1].Dimensions[1].Links.Options.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions/geography/options")
				So(results[1].Dimensions[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
				So(results[1].Downloads.CSV.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv")
				So(results[1].Downloads.CSV.Size, ShouldEqual, "15000")
				So(results[1].Downloads.CSVW.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json")
				So(results[1].Downloads.CSVW.Size, ShouldEqual, "30000")
				So(results[1].Downloads.TXT.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.txt")
				So(results[1].Downloads.TXT.Size, ShouldEqual, "45000")
				So(results[1].Downloads.XLS.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xls")
				So(results[1].Downloads.XLS.Size, ShouldEqual, "60000")
				So(results[1].Downloads.XLSX.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx")
				So(results[1].Downloads.XLSX.Size, ShouldEqual, "75000")
				So(results[1].Edition, ShouldEqual, "time-series")
				So(results[1].InstanceID, ShouldEqual, "2")
				So(results[1].Links.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(results[1].Links.Dimensions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				So(results[1].Links.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(results[1].Links.Job.HRef, ShouldEqual, "http://localhost:21800/jobs/2")
				So(results[1].Links.Self.HRef, ShouldEqual, "http://localhost:22000/instances/2")
				So(results[1].Links.Spatial.HRef, ShouldEqual, "http://oldhost:1000/spatial/2")
				So(results[1].Links.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the instance and download links are empty", func() {
			results := []*models.Instance{
				{
					CollectionID: "cantabularflexibledefault-1",
					Edition:      "2021",
					InstanceID:   "1",
					Links:        &models.InstanceLinks{},
					Downloads:    &models.DownloadList{},
				},
				{
					CollectionID: "cpihtest-1",
					Edition:      "time-series",
					InstanceID:   "2",
					Links:        &models.InstanceLinks{},
					Downloads:    &models.DownloadList{},
				},
			}

			err := RewriteInstances(ctx, results, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder, downloadServiceURL)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)

				So(results[0].CollectionID, ShouldEqual, "cantabularflexibledefault-1")
				So(results[0].Edition, ShouldEqual, "2021")
				So(results[0].InstanceID, ShouldEqual, "1")
				So(results[0].Links, ShouldResemble, &models.InstanceLinks{})
				So(results[0].Downloads, ShouldResemble, &models.DownloadList{})

				So(results[1].CollectionID, ShouldEqual, "cpihtest-1")
				So(results[1].Edition, ShouldEqual, "time-series")
				So(results[1].InstanceID, ShouldEqual, "2")
				So(results[1].Links, ShouldResemble, &models.InstanceLinks{})
				So(results[1].Downloads, ShouldResemble, &models.DownloadList{})
			})
		})

		Convey("When the instances are empty", func() {
			results := []*models.Instance{}

			err := RewriteInstances(ctx, results, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder, downloadServiceURL)

			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteInstances_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a list of instances", t, func() {
		importLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, importAPIURL)
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		codeListLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, codeListAPIURL)

		Convey("When the instance links are unable to be parsed", func() {
			results := []*models.Instance{
				{
					CollectionID: "cantabularflexibledefault-1",
					Edition:      "2021",
					InstanceID:   "1",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cantabular-flexible-default",
							ID:   "cantabular-flexible-default",
						},
						Dimensions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cantabular-flexible-default/editions/2021",
							ID:   "2021",
						},
						Job: &models.LinkObject{
							HRef: "://oldhost:1000/jobs/1",
							ID:   "1",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/instances/1",
						},
						Version: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cantabular-flexible-default/editions/2021/versions/1",
							ID:   "1",
						},
					},
				},
				{
					CollectionID: "cpihtest-1",
					Edition:      "time-series",
					InstanceID:   "2",
					Links: &models.InstanceLinks{
						Dataset: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01",
							ID:   "cpih01",
						},
						Dimensions: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
						},
						Edition: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
							ID:   "time-series",
						},
						Job: &models.LinkObject{
							HRef: "://oldhost:1000/jobs/2",
							ID:   "2",
						},
						Self: &models.LinkObject{
							HRef: "://oldhost:1000/instances/2",
						},
						Version: &models.LinkObject{
							HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
							ID:   "1",
						},
					},
				},
			}

			err := RewriteInstances(ctx, results, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder, downloadServiceURL)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the dimensions links are unable to be parsed", func() {
			results := []*models.Instance{
				{
					CollectionID: "cantabularflexibledefault-1",
					Dimensions: []models.Dimension{
						{
							Label: "City",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							HRef: "://oldhost:1000/city",
							ID:   "city",
						},
						{
							Label: "Number of siblings (3 mappings)",
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							HRef: "://oldhost:1000/siblings_3",
							ID:   "siblings_3",
						},
					},
					Edition:    "2021",
					InstanceID: "1",
				},
				{
					CollectionID: "cpihtest-1",
					Dimensions: []models.Dimension{
						{
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							HRef: "://oldhost:1000/code-lists/mmm-yy",
							ID:   "mmm-yy",
							Name: "time",
						},
						{
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							HRef: "://oldhost:1000/code-lists/uk-only",
							ID:   "uk-only",
							Name: "geography",
						},
						{
							Links: models.DimensionLink{
								CodeList: models.LinkObject{},
								Options:  models.LinkObject{},
								Version:  models.LinkObject{},
							},
							HRef: "://oldhost:1000/code-lists/cpih1dim1aggid",
							ID:   "cpih1dim1aggid",
							Name: "aggregate",
						},
					},
					Edition:    "time-series",
					InstanceID: "2",
				},
			}

			err := RewriteInstances(ctx, results, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder, downloadServiceURL)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the download links are unable to be parsed", func() {
			results := []*models.Instance{
				{
					CollectionID: "cantabularflexibledefault-1",
					Downloads: &models.DownloadList{
						CSV: &models.DownloadObject{
							HRef: "://oldhost:1000/downloads/cantabular-flexible-default/editions/2021/versions/1.csv",
							Size: "15000",
						},
						CSVW: &models.DownloadObject{
							HRef: "://oldhost:1000/downloads/cantabular-flexible-default/editions/2021/versions/1.csv-metadata.json",
							Size: "30000",
						},
						TXT: &models.DownloadObject{
							HRef: "://oldhost:1000/downloads/cantabular-flexible-default/editions/2021/versions/1.txt",
							Size: "45000",
						},
						XLS: &models.DownloadObject{
							HRef: "://oldhost:1000/downloads/cantabular-flexible-default/editions/2021/versions/1.xls",
							Size: "60000",
						},
						XLSX: &models.DownloadObject{
							HRef: "://oldhost:1000/downloads/cantabular-flexible-default/editions/2021/versions/1.xlsx",
							Size: "75000",
						},
					},
					Edition:    "2021",
					InstanceID: "1",
				},
			}

			err := RewriteInstances(ctx, results, datasetLinksBuilder, codeListLinksBuilder, importLinksBuilder, downloadServiceURL)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteInstanceLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of instance links", t, func() {
		importLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, importAPIURL)
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the instance links need rewriting", func() {
			instanceLinks := &models.InstanceLinks{
				Dataset: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series",
				},
				Job: &models.LinkObject{
					HRef: "https://oldhost:1000/jobs/1",
				},
				Self: &models.LinkObject{
					HRef: "https://oldhost:1000/instances/1",
				},
				Spatial: &models.LinkObject{
					HRef: "http://oldhost:1000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "https://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteInstanceLinks(ctx, instanceLinks, datasetLinksBuilder, importLinksBuilder)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(instanceLinks.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(instanceLinks.Dimensions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				So(instanceLinks.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(instanceLinks.Job.HRef, ShouldEqual, "http://localhost:21800/jobs/1")
				So(instanceLinks.Self.HRef, ShouldEqual, "http://localhost:22000/instances/1")
				So(instanceLinks.Spatial.HRef, ShouldEqual, "http://oldhost:1000/spatial")
				So(instanceLinks.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the instance links do not need rewriting", func() {
			instanceLinks := &models.InstanceLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
				},
				Job: &models.LinkObject{
					HRef: "http://localhost:21800/jobs/1",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/1",
				},
				Spatial: &models.LinkObject{
					HRef: "http://oldhost:1000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteInstanceLinks(ctx, instanceLinks, datasetLinksBuilder, importLinksBuilder)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(instanceLinks.Dataset.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01")
				So(instanceLinks.Dimensions.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions")
				So(instanceLinks.Edition.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series")
				So(instanceLinks.Job.HRef, ShouldEqual, "http://localhost:21800/jobs/1")
				So(instanceLinks.Self.HRef, ShouldEqual, "http://localhost:22000/instances/1")
				So(instanceLinks.Spatial.HRef, ShouldEqual, "http://oldhost:1000/spatial")
				So(instanceLinks.Version.HRef, ShouldEqual, "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1")
			})
		})

		Convey("When the instance links are empty", func() {
			instanceLinks := &models.InstanceLinks{}

			err := RewriteInstanceLinks(ctx, instanceLinks, datasetLinksBuilder, importLinksBuilder)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(instanceLinks, ShouldResemble, &models.InstanceLinks{})
			})
		})

		Convey("When the instance links are nil", func() {
			err := RewriteInstanceLinks(ctx, nil, datasetLinksBuilder, importLinksBuilder)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteInstanceLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of instance links", t, func() {
		importLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, importAPIURL)
		datasetLinksBuilder := links.FromHeadersOrDefault(&http.Header{}, datasetAPIURL)
		Convey("When the instance links are unable to be parsed", func() {
			instanceLinks := &models.InstanceLinks{
				Dataset: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series",
				},
				Job: &models.LinkObject{
					HRef: "://oldhost:1000/jobs/1",
				},
				Self: &models.LinkObject{
					HRef: "://oldhost:1000/instances/1",
				},
				Spatial: &models.LinkObject{
					HRef: "://oldhost:1000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "://oldhost:1000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteInstanceLinks(ctx, instanceLinks, nil, nil)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})

		Convey("When the Job link is unable to be parsed", func() {
			instanceLinks := &models.InstanceLinks{
				Dataset: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01",
				},
				Dimensions: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1/dimensions",
				},
				Edition: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series",
				},
				Job: &models.LinkObject{
					HRef: "://oldhost:1000/jobs/1",
				},
				Self: &models.LinkObject{
					HRef: "http://localhost:22000/instances/1",
				},
				Spatial: &models.LinkObject{
					HRef: "http://localhost:22000/spatial",
				},
				Version: &models.LinkObject{
					HRef: "http://localhost:22000/datasets/cpih01/editions/time-series/versions/1",
				},
			}

			err := RewriteInstanceLinks(ctx, instanceLinks, datasetLinksBuilder, importLinksBuilder)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDownloadLinks_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of download links", t, func() {
		Convey("When the download links need rewriting", func() {
			downloadLinks := &models.DownloadList{
				CSV: &models.DownloadObject{
					HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.csv",
					Size: "15000",
				},
				CSVW: &models.DownloadObject{
					HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json",
					Size: "30000",
				},
				TXT: &models.DownloadObject{
					HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.txt",
					Size: "45000",
				},
				XLS: &models.DownloadObject{
					HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.xls",
					Size: "60000",
				},
				XLSX: &models.DownloadObject{
					HRef: "https://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx",
					Size: "75000",
				},
			}

			err := RewriteDownloadLinks(ctx, downloadLinks, downloadServiceURL)

			Convey("Then the links should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So(downloadLinks.CSV.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv")
				So(downloadLinks.CSV.Size, ShouldEqual, "15000")
				So(downloadLinks.CSVW.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json")
				So(downloadLinks.CSVW.Size, ShouldEqual, "30000")
				So(downloadLinks.TXT.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.txt")
				So(downloadLinks.TXT.Size, ShouldEqual, "45000")
				So(downloadLinks.XLS.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xls")
				So(downloadLinks.XLS.Size, ShouldEqual, "60000")
				So(downloadLinks.XLSX.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx")
				So(downloadLinks.XLSX.Size, ShouldEqual, "75000")
			})
		})

		Convey("When the download links do not need rewriting", func() {
			downloadLinks := &models.DownloadList{
				CSV: &models.DownloadObject{
					HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv",
					Size: "15000",
				},
				CSVW: &models.DownloadObject{
					HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json",
					Size: "30000",
				},
				TXT: &models.DownloadObject{
					HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.txt",
					Size: "45000",
				},
				XLS: &models.DownloadObject{
					HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xls",
					Size: "60000",
				},
				XLSX: &models.DownloadObject{
					HRef: "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx",
					Size: "75000",
				},
			}

			err := RewriteDownloadLinks(ctx, downloadLinks, downloadServiceURL)

			Convey("Then the links should remain the same", func() {
				So(err, ShouldBeNil)
				So(downloadLinks.CSV.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv")
				So(downloadLinks.CSV.Size, ShouldEqual, "15000")
				So(downloadLinks.CSVW.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json")
				So(downloadLinks.CSVW.Size, ShouldEqual, "30000")
				So(downloadLinks.TXT.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.txt")
				So(downloadLinks.TXT.Size, ShouldEqual, "45000")
				So(downloadLinks.XLS.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xls")
				So(downloadLinks.XLS.Size, ShouldEqual, "60000")
				So(downloadLinks.XLSX.HRef, ShouldEqual, "http://localhost:23600/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx")
				So(downloadLinks.XLSX.Size, ShouldEqual, "75000")
			})
		})

		Convey("When the download links are empty", func() {
			downloadLinks := &models.DownloadList{}

			err := RewriteDownloadLinks(ctx, downloadLinks, downloadServiceURL)

			Convey("Then the links should remain empty", func() {
				So(err, ShouldBeNil)
				So(downloadLinks, ShouldResemble, &models.DownloadList{})
			})
		})

		Convey("When the download links are nil", func() {
			err := RewriteDownloadLinks(ctx, nil, downloadServiceURL)

			Convey("Then the links should remain nil", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}

func TestRewriteDownloadLinks_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of download links", t, func() {
		Convey("When the download links are unable to be parsed", func() {
			downloadLinks := &models.DownloadList{
				CSV: &models.DownloadObject{
					HRef: "://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.csv",
					Size: "15000",
				},
				CSVW: &models.DownloadObject{
					HRef: "://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.csv-metadata.json",
					Size: "30000",
				},
				TXT: &models.DownloadObject{
					HRef: "://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.txt",
					Size: "45000",
				},
				XLS: &models.DownloadObject{
					HRef: "://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.xls",
					Size: "60000",
				},
				XLSX: &models.DownloadObject{
					HRef: "://oldhost:1000/downloads/datasets/cpih01/editions/time-series/versions/1.xlsx",
					Size: "75000",
				},
			}

			err := RewriteDownloadLinks(ctx, downloadLinks, downloadServiceURL)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestRewriteDistributions_Success(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of distributions", t, func() {
		Convey("When the DownloadURLs need rewriting", func() {
			distributions := &[]models.Distribution{
				{
					Title:       "Distribution 1",
					Format:      "CSV",
					MediaType:   "text/csv",
					DownloadURL: "/cpih01/time-series/1/filename.csv",
					ByteSize:    10000,
				},
				{
					Title:       "Distribution 2",
					Format:      "XLSX",
					MediaType:   "text/xlsx",
					DownloadURL: "/cpih01/time-series/1/filename.xlsx",
					ByteSize:    20000,
				},
				{
					Title:       "Distribution 3",
					Format:      "XLS",
					MediaType:   "text/xls",
					DownloadURL: "/cpih01/time-series/1/filename.xls",
					ByteSize:    30000,
				},
			}

			distributions, err := RewriteDistributions(ctx, distributions, downloadServiceURL)

			Convey("Then the DownloadURLs should be rewritten correctly", func() {
				So(err, ShouldBeNil)
				So((*distributions)[0].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv")
				So((*distributions)[1].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx")
				So((*distributions)[2].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xls")
			})
		})

		Convey("When the DownloadURLs do not need rewriting", func() {
			distributions := &[]models.Distribution{
				{
					Title:       "Distribution 1",
					Format:      "CSV",
					MediaType:   "text/csv",
					DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv",
					ByteSize:    10000,
				},
				{
					Title:       "Distribution 2",
					Format:      "XLSX",
					MediaType:   "text/xlsx",
					DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx",
					ByteSize:    20000,
				},
				{
					Title:       "Distribution 3",
					Format:      "XLS",
					MediaType:   "text/xls",
					DownloadURL: "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xls",
					ByteSize:    30000,
				},
			}

			distributions, err := RewriteDistributions(ctx, distributions, downloadServiceURL)

			Convey("Then the DownloadURLs should remain the same", func() {
				So(err, ShouldBeNil)
				So((*distributions)[0].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.csv")
				So((*distributions)[1].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xlsx")
				So((*distributions)[2].DownloadURL, ShouldEqual, "http://localhost:23600/downloads/files/cpih01/time-series/1/filename.xls")
			})
		})

		Convey("When the distributions are empty", func() {
			distributions := &[]models.Distribution{}

			distributions, err := RewriteDistributions(ctx, distributions, downloadServiceURL)

			Convey("Then the distributions should remain empty", func() {
				So(err, ShouldBeNil)
				So(len(*distributions), ShouldEqual, 0)
			})
		})

		Convey("When the distributions are nil", func() {
			distributions := (*[]models.Distribution)(nil)

			distributions, err := RewriteDistributions(ctx, distributions, downloadServiceURL)

			Convey("Then the distributions should remain nil", func() {
				So(err, ShouldBeNil)
				So(distributions, ShouldBeNil)
			})
		})
	})
}

func TestRewriteDistributions_Error(t *testing.T) {
	ctx := context.Background()
	Convey("Given a set of distributions", t, func() {
		Convey("When the DownloadURLs are unable to be parsed", func() {
			distributions := &[]models.Distribution{
				{
					Title:       "Distribution 1",
					Format:      "CSV",
					MediaType:   "text/csv",
					DownloadURL: "://oldhost:1000/downloads/files/cpih01/time-series/1/filename.csv",
					ByteSize:    10000,
				},
			}

			_, err := RewriteDistributions(ctx, distributions, downloadServiceURL)

			Convey("Then a parsing error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "unable to parse link to URL")
			})
		})
	})
}

func TestPurgeCache(t *testing.T) {
	Convey("Given a dataset and edition", t, func() {
		ctx := context.Background()
		datasetID := "test-dataset"
		editionID := "test-edition"
		zoneID := "test-zone"
		apiToken := "test-token"
		baseURL := "http://base-url"

		Convey("When cache purge succeeds", func() {
			var capturedRequest *http.Request
			var capturedBody []byte

			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedRequest = r
				capturedBody, _ = io.ReadAll(r.Body)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"success": true}`))
			}))
			defer mockServer.Close()

			mockClient := &http.Client{
				Transport: &mockTransport{server: mockServer},
			}

			err := PurgeCache(ctx, datasetID, editionID, baseURL, zoneID, apiToken, mockClient)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})

			Convey("And the request has correct headers", func() {
				So(capturedRequest.Header.Get("Content-Type"), ShouldEqual, "application/json")
				So(capturedRequest.Header.Get("Authorization"), ShouldEqual, "Bearer test-token")
			})

			Convey("And the request contains all 6 URL prefixes", func() {
				var prefixes Prefixes
				json.Unmarshal(capturedBody, &prefixes)

				So(len(prefixes.Prefixes), ShouldEqual, 6)
				So(prefixes.Prefixes[0], ShouldEqual, "www.ons.gov.uk/datasets/test-dataset")
				So(prefixes.Prefixes[1], ShouldEqual, "www.ons.gov.uk/datasets/test-dataset/editions")
				So(prefixes.Prefixes[2], ShouldEqual, "www.ons.gov.uk/datasets/test-dataset/editions/test-edition/versions")
				So(prefixes.Prefixes[3], ShouldEqual, "api.beta.ons.gov.uk/v1/datasets/test-dataset")
				So(prefixes.Prefixes[4], ShouldEqual, "api.beta.ons.gov.uk/v1/datasets/test-dataset/editions")
				So(prefixes.Prefixes[5], ShouldEqual, "api.beta.ons.gov.uk/v1/datasets/test-dataset/editions/test-edition/versions")
			})
		})

		Convey("When Cloudflare returns an error", func() {
			mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(`{"error": "something went wrong"}`))
			}))
			defer mockServer.Close()

			mockClient := &http.Client{
				Transport: &mockTransport{server: mockServer},
			}

			err := PurgeCache(ctx, datasetID, editionID, baseURL, zoneID, apiToken, mockClient)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "cloudflare API returned status 500")
			})
		})
	})
}

func TestGenerateDistributionsDownloadURLs(t *testing.T) {
	Convey("Given a set of distributions", t, func() {
		distributions := &[]models.Distribution{
			{
				Title:       "Distribution 1",
				Format:      "CSV",
				MediaType:   "text/csv",
				DownloadURL: "/cpih01/time-series/1/file1.csv",
				ByteSize:    10000,
			},
			{
				Title:       "Distribution 2",
				Format:      "XLSX",
				MediaType:   "text/xlsx",
				DownloadURL: "/cpih01/time-series/1/file2.xlsx",
				ByteSize:    20000,
			},
			{
				Title:       "Distribution 3",
				Format:      "XLS",
				MediaType:   "text/xls",
				DownloadURL: "/cpih01/time-series/1/file3.xls",
				ByteSize:    30000,
			},
		}

		datasetID := "new-dataset"
		edition := "new-edition"
		version := 2

		Convey("When generating download URLs", func() {
			updatedDistributions := GenerateDistributionsDownloadURLs(datasetID, edition, version, distributions)

			expectedResults := &[]models.Distribution{
				{
					Title:       "Distribution 1",
					Format:      "CSV",
					MediaType:   "text/csv",
					DownloadURL: "/new-dataset/new-edition/2/file1.csv",
					ByteSize:    10000,
				},
				{
					Title:       "Distribution 2",
					Format:      "XLSX",
					MediaType:   "text/xlsx",
					DownloadURL: "/new-dataset/new-edition/2/file2.xlsx",
					ByteSize:    20000,
				},
				{
					Title:       "Distribution 3",
					Format:      "XLS",
					MediaType:   "text/xls",
					DownloadURL: "/new-dataset/new-edition/2/file3.xls",
					ByteSize:    30000,
				},
			}

			Convey("Then the DownloadURLs should be generated correctly using the provided parameters", func() {
				So(updatedDistributions, ShouldResemble, expectedResults)
			})
		})

		Convey("When the distributions are empty", func() {
			emptyDistributions := &[]models.Distribution{}

			updatedDistributions := GenerateDistributionsDownloadURLs(datasetID, edition, version, emptyDistributions)

			Convey("Then the distributions should remain empty", func() {
				So(len(*updatedDistributions), ShouldEqual, 0)
			})
		})

		Convey("When the distributions are nil", func() {
			var nilDistributions *[]models.Distribution

			updatedDistributions := GenerateDistributionsDownloadURLs(datasetID, edition, version, nilDistributions)

			Convey("Then the distributions should remain nil", func() {
				So(updatedDistributions, ShouldBeNil)
			})
		})
	})
}

type mockTransport struct {
	server *httptest.Server
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = "http"
	req.URL.Host = m.server.URL[7:]
	return http.DefaultTransport.RoundTrip(req)
}
