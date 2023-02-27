package models

import (
	"fmt"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/url"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateMetadata(t *testing.T) {
	Convey("Given a dataset and a version objects", t, func() {
		dataset := Dataset{
			ID:             "9875",
			CanonicalTopic: "1234",
			Subtopics:      []string{"5678", "9012"},
			Description:    "census",
			Keywords:       []string{"test", "test2"},
			Title:          "CensusEthnicity",
			UnitOfMeasure:  "Pounds Sterling",
			Contacts:       []ContactDetails{contacts},
			URI:            "http://localhost:22000/datasets/123/breadcrumbs",
			QMI:            &qmi,
			RelatedContent: []GeneralDetails{relatedDatasets},
			License:        "license",
			Methodologies: []GeneralDetails{
				methodology,
			},
			NationalStatistic: &nationalStatistic,
			NextRelease:       "2023-03-14",
			Publications: []GeneralDetails{
				publications,
			},
			Publisher: &publisher,
			RelatedDatasets: []GeneralDetails{
				relatedDatasets,
			},
			ReleaseFrequency: "yearly",
			Theme:            "population",
			Links: &DatasetLinks{
				AccessRights: &LinkObject{
					HRef: "href-access-rights",
					ID:   "access-rights",
				},
				Editions: &LinkObject{
					HRef: "href-editions",
					ID:   "editions",
				},
				LatestVersion: &LinkObject{
					HRef: "href-latest",
					ID:   "latest",
				},
				Self: &LinkObject{
					HRef: "href-self",
					ID:   "self",
				},
				Taxonomy: &LinkObject{
					HRef: "href-taxonomy",
					ID:   "taxonomy",
				},
			},
		}

		csvDownload := DownloadObject{
			HRef:    "https://www.aws/123csv",
			Private: "csv-private",
			Public:  "csv-public",
			Size:    "252",
		}
		csvwDownload := DownloadObject{
			HRef:    "https://www.aws/123",
			Private: "csvw-private",
			Public:  "csvw-public",
			Size:    "25",
		}
		xlsDownload := DownloadObject{
			HRef:    "https://www.aws/1234",
			Private: "xls-private",
			Public:  "xls-public",
			Size:    "45",
		}
		txtDownload := DownloadObject{
			HRef:    "https://www.aws/txt",
			Private: "txt-private",
			Public:  "txt-public",
			Size:    "11",
		}
		xlsxDownload := DownloadObject{
			HRef:    "https://www.aws/xlsx",
			Private: "xlsx-private",
			Public:  "xlsx-public",
			Size:    "119",
		}

		version := Version{
			Alerts:     &[]Alert{alert},
			Dimensions: []Dimension{dimension},
			Downloads: &DownloadList{
				CSV:  &csvDownload,
				CSVW: &csvwDownload,
				XLS:  &xlsDownload,
				TXT:  &txtDownload,
				XLSX: &xlsxDownload,
			},
			Edition:       "2017",
			Headers:       []string{"cantabular_table", "age"},
			LatestChanges: &[]LatestChange{latestChange},
			Links:         &links,
			ReleaseDate:   "2017-10-12",
			State:         PublishedState,
			Temporal:      &[]TemporalFrequency{temporal},
			Version:       1,
		}

		Convey("When we call CreateCantabularMetaDataDoc", func() {
			var urlBuilder *url.Builder = nil // Not used for cantabular
			metaDataDoc := CreateCantabularMetaDataDoc(&dataset, &version, urlBuilder)

			Convey("Then it returns a metadata object with all the Cantabular fields populated", func() {
				So(metaDataDoc.Description, ShouldEqual, dataset.Description)
				So(metaDataDoc.Keywords, ShouldResemble, dataset.Keywords)
				So(metaDataDoc.Title, ShouldEqual, dataset.Title)
				So(metaDataDoc.UnitOfMeasure, ShouldEqual, dataset.UnitOfMeasure)
				So(metaDataDoc.Contacts, ShouldResemble, dataset.Contacts)
				So(metaDataDoc.URI, ShouldEqual, dataset.URI)
				So(metaDataDoc.QMI, ShouldResemble, dataset.QMI)
				So(metaDataDoc.DatasetLinks, ShouldResemble, dataset.Links)
				So(metaDataDoc.RelatedContent, ShouldResemble, dataset.RelatedContent)
				So(metaDataDoc.CanonicalTopic, ShouldEqual, dataset.CanonicalTopic)
				So(metaDataDoc.Subtopics, ShouldResemble, dataset.Subtopics)

				So(metaDataDoc.CSVHeader, ShouldResemble, version.Headers)
				So(metaDataDoc.Dimensions, ShouldResemble, version.Dimensions)
				So(metaDataDoc.ReleaseDate, ShouldEqual, version.ReleaseDate)
				So(metaDataDoc.Version, ShouldEqual, version.Version)

				So(metaDataDoc.Downloads.CSV.HRef, ShouldEqual, csvDownload.HRef)
				So(metaDataDoc.Downloads.CSV.Size, ShouldEqual, csvDownload.Size)
				So(metaDataDoc.Downloads.CSV.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.CSV.Public, ShouldEqual, "")
				So(metaDataDoc.Downloads.CSVW.HRef, ShouldEqual, csvwDownload.HRef)
				So(metaDataDoc.Downloads.CSVW.Size, ShouldEqual, csvwDownload.Size)
				So(metaDataDoc.Downloads.CSVW.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.CSVW.Public, ShouldEqual, "")
				So(metaDataDoc.Downloads.TXT.HRef, ShouldEqual, txtDownload.HRef)
				So(metaDataDoc.Downloads.TXT.Size, ShouldEqual, txtDownload.Size)
				So(metaDataDoc.Downloads.TXT.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.TXT.Public, ShouldEqual, "")
				So(metaDataDoc.Downloads.XLS.HRef, ShouldEqual, xlsDownload.HRef)
				So(metaDataDoc.Downloads.XLS.Size, ShouldEqual, xlsDownload.Size)
				So(metaDataDoc.Downloads.XLS.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.XLS.Public, ShouldEqual, "")
				So(metaDataDoc.Downloads.XLSX.HRef, ShouldEqual, xlsxDownload.HRef)
				So(metaDataDoc.Downloads.XLSX.Size, ShouldEqual, xlsxDownload.Size)
				So(metaDataDoc.Downloads.XLSX.Private, ShouldEqual, xlsxDownload.Private) // TODO: Should it be cleared?
				So(metaDataDoc.Downloads.XLSX.Public, ShouldEqual, xlsxDownload.Public)   // TODO: Should it be cleared?

				// TODO: Should it include xlsx?
				So(metaDataDoc.Distribution, ShouldResemble, []string{"json", "csv", "csvw", "xls", "txt"})
			})

			Convey("And the non-Cantabular fields are empty", func() {
				So(metaDataDoc.Alerts, ShouldBeNil)
				So(metaDataDoc.LatestChanges, ShouldBeNil)
				So(metaDataDoc.Links, ShouldBeNil)
				So(metaDataDoc.License, ShouldEqual, "")
				So(metaDataDoc.Methodologies, ShouldBeNil)
				So(metaDataDoc.NationalStatistic, ShouldBeNil)
				So(metaDataDoc.NextRelease, ShouldEqual, "")
				So(metaDataDoc.Publications, ShouldBeNil)
				So(metaDataDoc.Publisher, ShouldBeNil)
				So(metaDataDoc.RelatedDatasets, ShouldBeNil)
				So(metaDataDoc.ReleaseFrequency, ShouldEqual, "")
				So(metaDataDoc.Temporal, ShouldBeNil)
				So(metaDataDoc.Theme, ShouldEqual, "")
				So(metaDataDoc.UsageNotes, ShouldBeNil)
			})
		})

		Convey("When we call CreateMetaDataDoc", func() {
			websiteUrl := "http://localhost:20000"
			metaDataDoc := CreateMetaDataDoc(&dataset, &version, url.NewBuilder(websiteUrl))

			Convey("Then it returns a metadata object with all the CMD fields populated", func() {
				So(metaDataDoc.Description, ShouldEqual, dataset.Description)
				So(metaDataDoc.Keywords, ShouldResemble, dataset.Keywords)
				So(metaDataDoc.Title, ShouldEqual, dataset.Title)
				So(metaDataDoc.UnitOfMeasure, ShouldEqual, dataset.UnitOfMeasure)
				So(metaDataDoc.Contacts, ShouldResemble, dataset.Contacts)
				So(metaDataDoc.License, ShouldResemble, dataset.License)
				So(metaDataDoc.Methodologies, ShouldResemble, dataset.Methodologies)
				So(metaDataDoc.NationalStatistic, ShouldResemble, dataset.NationalStatistic)
				So(metaDataDoc.NextRelease, ShouldResemble, dataset.NextRelease)
				So(metaDataDoc.Publications, ShouldResemble, dataset.Publications)
				So(metaDataDoc.Publisher, ShouldResemble, dataset.Publisher)
				So(metaDataDoc.RelatedDatasets, ShouldResemble, dataset.RelatedDatasets)
				So(metaDataDoc.ReleaseFrequency, ShouldResemble, dataset.ReleaseFrequency)
				So(metaDataDoc.Theme, ShouldResemble, dataset.Theme)
				So(metaDataDoc.URI, ShouldEqual, dataset.URI)
				So(metaDataDoc.QMI, ShouldResemble, dataset.QMI)
				So(metaDataDoc.CanonicalTopic, ShouldEqual, dataset.CanonicalTopic)
				So(metaDataDoc.Subtopics, ShouldResemble, dataset.Subtopics)
				So(metaDataDoc.Links.AccessRights, ShouldEqual, dataset.Links.AccessRights)

				So(metaDataDoc.Alerts, ShouldEqual, version.Alerts)
				So(metaDataDoc.Dimensions, ShouldResemble, version.Dimensions)
				So(metaDataDoc.LatestChanges, ShouldEqual, version.LatestChanges)
				So(metaDataDoc.ReleaseDate, ShouldEqual, version.ReleaseDate)
				So(metaDataDoc.Temporal, ShouldEqual, version.Temporal)
				So(metaDataDoc.UsageNotes, ShouldEqual, version.UsageNotes)

				So(metaDataDoc.Downloads.CSV.HRef, ShouldEqual, csvDownload.HRef)
				So(metaDataDoc.Downloads.CSV.Size, ShouldEqual, csvDownload.Size)
				So(metaDataDoc.Downloads.CSV.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.CSV.Public, ShouldEqual, "")
				So(metaDataDoc.Downloads.CSVW.HRef, ShouldEqual, csvwDownload.HRef)
				So(metaDataDoc.Downloads.CSVW.Size, ShouldEqual, csvwDownload.Size)
				So(metaDataDoc.Downloads.CSVW.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.CSVW.Public, ShouldEqual, "")
				So(metaDataDoc.Downloads.TXT.HRef, ShouldEqual, txtDownload.HRef)
				So(metaDataDoc.Downloads.TXT.Size, ShouldEqual, txtDownload.Size)
				So(metaDataDoc.Downloads.TXT.Private, ShouldEqual, txtDownload.Private) // TODO: Should it be cleared?
				So(metaDataDoc.Downloads.TXT.Public, ShouldEqual, txtDownload.Public)   // TODO: Should it be cleared?
				So(metaDataDoc.Downloads.XLS.HRef, ShouldEqual, xlsDownload.HRef)
				So(metaDataDoc.Downloads.XLS.Size, ShouldEqual, xlsDownload.Size)
				So(metaDataDoc.Downloads.XLS.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.XLS.Public, ShouldEqual, "")
				So(metaDataDoc.Downloads.XLSX.Private, ShouldEqual, xlsxDownload.Private) // TODO: Should it be cleared?
				So(metaDataDoc.Downloads.XLSX.Public, ShouldEqual, xlsxDownload.Public)   // TODO: Should it be cleared?

				So(metaDataDoc.Links.Self.HRef, ShouldEqual, version.Links.Version.HRef+"/metadata")
				So(metaDataDoc.Links.Self.ID, ShouldEqual, "")
				So(metaDataDoc.Links.Spatial, ShouldEqual, version.Links.Spatial)
				So(metaDataDoc.Links.Version, ShouldEqual, version.Links.Version)
				expectedWebsiteHref := fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%d",
					websiteUrl, dataset.ID, version.Links.Edition.ID, version.Version)
				So(metaDataDoc.Links.WebsiteVersion.HRef, ShouldEqual, expectedWebsiteHref)
				So(metaDataDoc.Links.WebsiteVersion.ID, ShouldEqual, "")

				// TODO: Should it include xlsx?
				So(metaDataDoc.Distribution, ShouldResemble, []string{"json", "csv", "csvw", "xls", "txt"})
			})

			Convey("And the non-CMD fields are empty", func() {
				So(metaDataDoc.CSVHeader, ShouldBeNil)
				So(metaDataDoc.DatasetLinks, ShouldBeNil)
				So(metaDataDoc.RelatedContent, ShouldBeNil)
				So(metaDataDoc.Version, ShouldEqual, 0)
			})
		})
	})

}
