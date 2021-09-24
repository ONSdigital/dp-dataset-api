package models

import (
	"testing"

	"github.com/ONSdigital/dp-dataset-api/url"
	. "github.com/smartystreets/goconvey/convey"
)

var urlBuilder = url.NewBuilder("http://localhost:20000")

func TestCreateMetadataDoc(t *testing.T) {
	t.Parallel()

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

		exectedMetadataDoc := expectedMetadataDoc()

		metaDataDoc := CreateMetaDataDoc(inputDatasetDoc, inputVersionDoc, urlBuilder)
		So(metaDataDoc, ShouldResemble, &exectedMetadataDoc)
	})
}

func TestCreateCantabularMetadataDoc(t *testing.T) {

	Convey("Successfully create metadata document with all relavant cantabular fields", t, func() {
		inputDatasetDoc := &Dataset{
			Description:   "census",
			Keywords:      []string{"test", "test2"},
			Title:         "CensusEthnicity",
			UnitOfMeasure: "Pounds Sterling",
		}

		inputVersionDoc := &publishedVersion

		exectedCantabularMetadataDoc := expectedCantabularMetadataDoc()

		metaDataDoc := CreateCantabularMetaDataDoc(inputDatasetDoc, inputVersionDoc, urlBuilder)
		So(metaDataDoc, ShouldResemble, &exectedCantabularMetadataDoc)
	})
}
