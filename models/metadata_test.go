package models

import (
	"testing"

	"github.com/ONSdigital/dp-dataset-api/url"
	. "github.com/smartystreets/goconvey/convey"
)

var urlBuilder = url.NewBuilder("http://localhost:20000")

func TestCreateMetadataDoc(t *testing.T) {

	Convey("Successfully create metadata document with title only", t, func() {

		inputDatasetDoc := &Dataset{
			Title: "CPI",
		}

		inputVersionDoc := &Version{}

		metaDataDoc := CreateMetaDataDoc(inputDatasetDoc, inputVersionDoc, urlBuilder)
		So(metaDataDoc.Title, ShouldEqual, "CPI")
	})

	Convey("Successfully create metadata document with all fields", t, func() {
		inputDatasetDoc := createTestDataset()

		inputVersionDoc := &publishedVersion

		expectedMetadataDoc := expectedMetadataDoc()

		metaDataDoc := CreateMetaDataDoc(inputDatasetDoc, inputVersionDoc, urlBuilder)
		So(metaDataDoc, ShouldResemble, &expectedMetadataDoc)
	})
}

func TestCreateCantabularMetadataDoc(t *testing.T) {

	Convey("Given a dataset and a version objects", t, func() {
		dataset := Dataset{
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

		version := Version{
			Alerts:     &[]Alert{alert},
			Dimensions: []Dimension{dimension},
			Downloads: &DownloadList{
				CSV: &DownloadObject{
					HRef: "https://www.aws/123",
					Size: "25",
				},
				CSVW: &DownloadObject{
					HRef: "https://www.aws/123",
					Size: "25",
				},
				XLS: &DownloadObject{
					HRef: "https://www.aws/1234",
					Size: "45",
				},
				TXT: &DownloadObject{
					HRef: "https://www.aws/txt",
					Size: "11",
				},
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
			metaDataDoc := CreateCantabularMetaDataDoc(&dataset, &version, urlBuilder)

			Convey("Then it returns a metadata document with all the Cantabular fields populated", func() {
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
				So(metaDataDoc.Downloads, ShouldEqual, version.Downloads)
				So(metaDataDoc.ReleaseDate, ShouldEqual, version.ReleaseDate)
				So(metaDataDoc.Version, ShouldResemble, version.Version)
				So(metaDataDoc.Distribution, ShouldResemble, []string{"json", "csv", "csvw", "xls", "txt"})
			})

			Convey("And the public and private download links are empty", func() {
				So(metaDataDoc.Downloads.CSV.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.CSV.Public, ShouldEqual, "")
				So(metaDataDoc.Downloads.XLS.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.XLS.Public, ShouldEqual, "")
				So(metaDataDoc.Downloads.TXT.Private, ShouldEqual, "")
				So(metaDataDoc.Downloads.TXT.Public, ShouldEqual, "")
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
	})

}
