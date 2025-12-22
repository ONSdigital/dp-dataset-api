package utils

import (
	"testing"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
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
