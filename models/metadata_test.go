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
		inputDatasetDoc := &Dataset{
			CanonicalTopic: "1234",
			Description:    "census",
			Keywords:       []string{"test", "test2"},
			Subtopics:      []string{"5678", "9012"},
			Title:          "CensusEthnicity",
			UnitOfMeasure:  "Pounds Sterling",
		}

		inputVersionDoc := &publishedVersion

		expectedCantabularMetadataDoc := expectedCantabularMetadataDoc()

		inputVersionDoc.Downloads = &cantabularDownloads

		metaDataDoc := CreateCantabularMetaDataDoc(inputDatasetDoc, inputVersionDoc, urlBuilder)
		So(metaDataDoc, ShouldResemble, &expectedCantabularMetadataDoc)
		So(metaDataDoc.Downloads.CSV.Private, ShouldResemble, "")
		So(metaDataDoc.Downloads.CSVW.Private, ShouldResemble, "")
		So(metaDataDoc.Downloads.XLS.Private, ShouldResemble, "")
	})

}
