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

	Convey("Successfully create metadata document with all relavant cantabular fields", t, func() {
		inputDatasetDoc := Dataset{
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

		inputVersionDoc := Version{
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

		metaDataDoc := CreateCantabularMetaDataDoc(&inputDatasetDoc, &inputVersionDoc, urlBuilder)
		So(metaDataDoc.Description, ShouldEqual, inputDatasetDoc.Description)
		So(metaDataDoc.Keywords, ShouldResemble, inputDatasetDoc.Keywords)
		So(metaDataDoc.Title, ShouldEqual, inputDatasetDoc.Title)
		So(metaDataDoc.UnitOfMeasure, ShouldEqual, inputDatasetDoc.UnitOfMeasure)
		So(metaDataDoc.Contacts, ShouldResemble, inputDatasetDoc.Contacts)
		So(metaDataDoc.URI, ShouldEqual, inputDatasetDoc.URI)
		So(metaDataDoc.QMI, ShouldResemble, inputDatasetDoc.QMI)
		So(metaDataDoc.DatasetLinks, ShouldResemble, inputDatasetDoc.Links)
		So(metaDataDoc.RelatedContent, ShouldResemble, inputDatasetDoc.RelatedContent)
		So(metaDataDoc.CanonicalTopic, ShouldEqual, inputDatasetDoc.CanonicalTopic)
		So(metaDataDoc.Subtopics, ShouldResemble, inputDatasetDoc.Subtopics)

		So(metaDataDoc.CSVHeader, ShouldResemble, inputVersionDoc.Headers)
		So(metaDataDoc.Dimensions, ShouldResemble, inputVersionDoc.Dimensions)
		So(metaDataDoc.Downloads, ShouldEqual, inputVersionDoc.Downloads)
		So(metaDataDoc.ReleaseDate, ShouldEqual, inputVersionDoc.ReleaseDate)
		So(metaDataDoc.Version, ShouldResemble, inputVersionDoc.Version)
		So(metaDataDoc.Distribution, ShouldResemble, []string{"json", "csv", "csvw", "xls", "txt"})

		So(metaDataDoc.Downloads.CSV.Private, ShouldEqual, "")
		So(metaDataDoc.Downloads.CSV.Public, ShouldEqual, "")
		So(metaDataDoc.Downloads.XLS.Private, ShouldEqual, "")
		So(metaDataDoc.Downloads.XLS.Public, ShouldEqual, "")
		So(metaDataDoc.Downloads.TXT.Private, ShouldEqual, "")
		So(metaDataDoc.Downloads.TXT.Public, ShouldEqual, "")
	})

}
