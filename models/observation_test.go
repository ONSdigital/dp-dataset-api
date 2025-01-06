package models

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestCreateObservationsDoc(t *testing.T) {
	query := "geography=K00001&age=*"

	queryParams := map[string]string{
		"geography": "K00001",
		"age":       "*",
	}

	t.Parallel()
	convey.Convey("Successfully create observations document with all fields", t, func() {
		// Setup test data
		datasetDoc := &Dataset{
			UnitOfMeasure: "Pounds Sterling",
		}

		versionDoc := setUpTestVersionDoc()

		observations := setUpTestObservations()

		observationsDoc := CreateObservationsDoc(query, versionDoc, datasetDoc, observations, queryParams, 0, 10000)
		convey.So(len(observationsDoc.Dimensions), convey.ShouldEqual, 1)
		convey.So(observationsDoc.Dimensions["geography"].LinkObject.ID, convey.ShouldEqual, "K00001")
		convey.So(observationsDoc.Dimensions["geography"].LinkObject.HRef, convey.ShouldEqual, "http://localhost:8080/codelists/123/codes/K00001")
		convey.So(observationsDoc.Limit, convey.ShouldEqual, 10000)
		convey.So(observationsDoc.Links.DatasetMetadata.HRef, convey.ShouldEqual, "http://localhost:8080/datasets/123/editions/2017/versions/1/metadata")
		convey.So(observationsDoc.Links.Self.HRef, convey.ShouldEqual, "http://localhost:8080/datasets/123/editions/2017/versions/1/observations?geography=K00001&age=*")
		convey.So(observationsDoc.Links.Version.HRef, convey.ShouldEqual, "http://localhost:8080/datasets/123/editions/2017/versions/1")
		convey.So(observationsDoc.Links.Version.ID, convey.ShouldEqual, "1")
		convey.So(len(observationsDoc.Observations), convey.ShouldEqual, 2)

		for i := 0; i < len(observationsDoc.Observations); i++ {
			observation := observationsDoc.Observations[i]
			if observation.Observation == "330" {
				convey.So(len(observation.Dimensions), convey.ShouldEqual, 1)
				convey.So(observation.Dimensions["age"].HRef, convey.ShouldEqual, "http://localhost:8080/codelists/456/codes/UTR234")
				convey.So(observation.Dimensions["age"].ID, convey.ShouldEqual, "UTR234")
				convey.So(observation.Dimensions["age"].Label, convey.ShouldEqual, "0-30")
				convey.So(len(observation.Metadata), convey.ShouldEqual, 2)
				convey.So(observation.Metadata["confidence interval"], convey.ShouldEqual, "0.7")
				convey.So(observation.Metadata["data marking"], convey.ShouldEqual, "")
				convey.So(observation.Observation, convey.ShouldEqual, "330")
			} else {
				convey.So(len(observation.Dimensions), convey.ShouldEqual, 1)
				convey.So(observation.Dimensions["age"].HRef, convey.ShouldEqual, "http://localhost:8080/codelists/456/codes/UTR567")
				convey.So(observation.Dimensions["age"].ID, convey.ShouldEqual, "UTR567")
				convey.So(observation.Dimensions["age"].Label, convey.ShouldEqual, "30+")
				convey.So(len(observation.Metadata), convey.ShouldEqual, 2)
				convey.So(observation.Metadata["confidence interval"], convey.ShouldEqual, "0.9")
				convey.So(observation.Metadata["data marking"], convey.ShouldEqual, "p")
				convey.So(observation.Observation, convey.ShouldEqual, "155")
			}
		}

		convey.So(observationsDoc.Offset, convey.ShouldEqual, 0)
		convey.So(observationsDoc.TotalObservations, convey.ShouldEqual, 2)
		convey.So(observationsDoc.UnitOfMeasure, convey.ShouldEqual, "Pounds Sterling")
		convey.So(observationsDoc.UsageNotes, convey.ShouldNotBeNil)
		convey.So(observationsDoc.UsageNotes, convey.ShouldResemble, &[]UsageNote{
			{Title: "Confidence Interval", Note: "A value identifying the level of confidence of the observational data"},
			{Title: "data marking", Note: "The marking of observational data?"},
		})
	})
}

func setUpTestVersionDoc() *Version {
	confidenceIntervalUsageNote := UsageNote{
		Title: "Confidence Interval",
		Note:  "A value identifying the level of confidence of the observational data",
	}

	dataMarkingUsageNote := UsageNote{
		Title: "data marking",
		Note:  "The marking of observational data?",
	}

	usageNotes := []UsageNote{confidenceIntervalUsageNote, dataMarkingUsageNote}

	geographyCode := Dimension{
		Name: "geography",
		HRef: "http://localhost:8080/codelists/123",
	}

	ageCode := Dimension{
		Name: "age",
		HRef: "http://localhost:8080/codelists/456",
	}

	versionDoc := &Version{
		Dimensions: []Dimension{geographyCode, ageCode},
		Headers:    []string{"geography, age"},
		Links: &VersionLinks{
			Version: &LinkObject{
				HRef: "http://localhost:8080/datasets/123/editions/2017/versions/1",
				ID:   "1",
			},
		},
		UsageNotes: &usageNotes,
	}

	return versionDoc
}

func setUpTestObservations() []Observation {
	ageDimensionOver30 := &DimensionObject{
		HRef:  "http://localhost:8080/codelists/456/codes/UTR567",
		ID:    "UTR567",
		Label: "30+",
	}

	observationDimensionsOne := make(map[string]*DimensionObject)
	observationDimensionsOne["age"] = ageDimensionOver30

	metadataOne := make(map[string]string)
	metadataOne["data marking"] = "p"
	metadataOne["confidence interval"] = "0.9"

	observationOne := Observation{
		Dimensions:  observationDimensionsOne,
		Metadata:    metadataOne,
		Observation: "155",
	}

	ageDimensionUnder30 := &DimensionObject{
		HRef:  "http://localhost:8080/codelists/456/codes/UTR234",
		ID:    "UTR234",
		Label: "0-30",
	}

	observationDimensionsTwo := make(map[string]*DimensionObject)
	observationDimensionsTwo["age"] = ageDimensionUnder30

	metadataTwo := make(map[string]string)
	metadataTwo["data marking"] = ""
	metadataTwo["confidence interval"] = "0.7"

	observationTwo := Observation{
		Dimensions:  observationDimensionsTwo,
		Metadata:    metadataTwo,
		Observation: "330",
	}

	observations := []Observation{observationOne, observationTwo}

	return observations
}
