package models

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateMetadataDoc(t *testing.T) {
	t.Parallel()

	Convey("Successfully create metadata document with title only", t, func() {

		inputDatasetDoc := &Dataset{
			Title: "CPI",
		}

		inputVersionDoc := &Version{}

		metaDataDoc := CreateMetaDataDoc(inputDatasetDoc, inputVersionDoc)
		So(metaDataDoc.Title, ShouldEqual, "CPI")
	})

	Convey("Successfully create metadata document with all fields", t, func() {
		inputDatasetDoc := createTestDataset()

		inputVersionDoc := &publishedVersion

		exectedMetadataDoc := expectedMetadataDoc()

		metaDataDoc := CreateMetaDataDoc(inputDatasetDoc, inputVersionDoc)
		So(metaDataDoc, ShouldResemble, &exectedMetadataDoc)
	})
}
