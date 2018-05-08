package models

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateObservationsDoc(t *testing.T) {
	query := "geography=K00001&age=*"

	queryParams := make(map[string]string)
	queryParams["geography"] = "K00001"
	queryParams["age"] = "*"

	t.Parallel()
	Convey("Successfully create observations document with all fields", t, func() {

		// Setup test data
		datasetDoc := &Dataset{
			UnitOfMeasure: "Pounds Sterling",
		}

		versionDoc := setUpTestVersionDoc()

		observations := setUpTestObservations()

		observationsDoc := CreateObservationsDoc(query, versionDoc, datasetDoc, observations, queryParams, 0, 10000)
		So(len(observationsDoc.Dimensions), ShouldEqual, 1)
		So(observationsDoc.Dimensions["geography"].LinkObject.ID, ShouldEqual, "K00001")
		So(observationsDoc.Dimensions["geography"].LinkObject.HRef, ShouldEqual, "http://localhost:8080/codelists/123/codes/K00001")
		So(observationsDoc.Limit, ShouldEqual, 10000)
		So(observationsDoc.Links.DatasetMetadata.HRef, ShouldEqual, "http://localhost:8080/datasets/123/editions/2017/versions/1/metadata")
		So(observationsDoc.Links.Self.HRef, ShouldEqual, "http://localhost:8080/datasets/123/editions/2017/versions/1/observations?geography=K00001&age=*")
		So(observationsDoc.Links.Version.HRef, ShouldEqual, "http://localhost:8080/datasets/123/editions/2017/versions/1")
		So(observationsDoc.Links.Version.ID, ShouldEqual, "1")
		So(len(observationsDoc.Observations), ShouldEqual, 2)

		for i := 0; i < len(observationsDoc.Observations); i++ {
			observation := observationsDoc.Observations[i]
			if observation.Observation == "330" {
				So(len(observation.Dimensions), ShouldEqual, 1)
				So(observation.Dimensions["age"].HRef, ShouldEqual, "http://localhost:8080/codelists/456/codes/UTR234")
				So(observation.Dimensions["age"].ID, ShouldEqual, "UTR234")
				So(observation.Dimensions["age"].Label, ShouldEqual, "0-30")
				So(len(observation.Metadata), ShouldEqual, 2)
				So(observation.Metadata["confidence interval"], ShouldEqual, "0.7")
				So(observation.Metadata["data marking"], ShouldEqual, "")
				So(observation.Observation, ShouldEqual, "330")
			} else {
				So(len(observation.Dimensions), ShouldEqual, 1)
				So(observation.Dimensions["age"].HRef, ShouldEqual, "http://localhost:8080/codelists/456/codes/UTR567")
				So(observation.Dimensions["age"].ID, ShouldEqual, "UTR567")
				So(observation.Dimensions["age"].Label, ShouldEqual, "30+")
				So(len(observation.Metadata), ShouldEqual, 2)
				So(observation.Metadata["confidence interval"], ShouldEqual, "0.9")
				So(observation.Metadata["data marking"], ShouldEqual, "p")
				So(observation.Observation, ShouldEqual, "155")
			}
		}

		So(observationsDoc.Offset, ShouldEqual, 0)
		So(observationsDoc.TotalObservations, ShouldEqual, 2)
		So(observationsDoc.UnitOfMeasure, ShouldEqual, "Pounds Sterling")
		So(observationsDoc.UsageNotes, ShouldNotBeNil)
		So(observationsDoc.UsageNotes, ShouldResemble, &[]UsageNote{
			UsageNote{Title: "Confidence Interval", Note: "A value identifying the level of confidence of the observational data"},
			UsageNote{Title: "data marking", Note: "The marking of observational data?"},
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

	geographyCode := CodeList{
		Name: "geography",
		HRef: "http://localhost:8080/codelists/123",
	}

	ageCode := CodeList{
		Name: "age",
		HRef: "http://localhost:8080/codelists/456",
	}

	versionDoc := &Version{
		Dimensions: []CodeList{geographyCode, ageCode},
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
