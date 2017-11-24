package models

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/ONSdigital/go-ns/log"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateDataset(t *testing.T) {
	t.Parallel()
	Convey("Successfully return without any errors", t, func() {
		Convey("when the dataset has all fields", func() {
			b, err := json.Marshal(inputDataset)
			if err != nil {
				log.ErrorC("Failed to marshal test data into bytes", err, nil)
				os.Exit(1)
			}
			r := bytes.NewReader(b)
			dataset, err := CreateDataset(PostMethod, r)
			So(err, ShouldBeNil)
			So(dataset.Links.AccessRights.HRef, ShouldEqual, "http://ons.gov.uk/accessrights")
			So(dataset.CollectionID, ShouldEqual, collectionID)
			So(dataset.Contacts[0], ShouldResemble, contacts)
			So(dataset.Description, ShouldEqual, "census")
			So(dataset.ID, ShouldNotBeNil)
			So(dataset.Keywords[0], ShouldEqual, "test")
			So(dataset.Keywords[1], ShouldEqual, "test2")
			So(dataset.License, ShouldEqual, "Office of National Statistics license")
			So(dataset.Methodologies[0], ShouldResemble, methodology)
			So(dataset.NationalStatistic, ShouldResemble, &nationalStatistic)
			So(dataset.NextRelease, ShouldEqual, "2016-05-05")
			So(dataset.Publications[0], ShouldResemble, publications)
			So(dataset.Publisher, ShouldResemble, &publisher)
			So(dataset.QMI, ShouldResemble, &qmi)
			So(dataset.RelatedDatasets[0], ShouldResemble, relatedDatasets)
			So(dataset.ReleaseFrequency, ShouldEqual, "yearly")
			So(dataset.State, ShouldEqual, "created")
			So(dataset.Theme, ShouldEqual, "population")
			So(dataset.Title, ShouldEqual, "CensusEthnicity")
			So(dataset.URI, ShouldEqual, "http://localhost:22000/datasets/123/breadcrumbs")
		})
	})

	Convey("Return with error when the request body contains the correct fields but of the wrong type", t, func() {
		b, err := json.Marshal(badInputData)
		if err != nil {
			log.ErrorC("Failed to marshal test data into bytes", err, nil)
			os.Exit(1)
		}
		r := bytes.NewReader(b)
		version, err := CreateDataset(PostMethod, r)
		So(version, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(err, ShouldResemble, errors.New("Failed to parse json body"))
	})
}

func TestCreateVersion(t *testing.T) {
	t.Parallel()
	Convey("Successfully return without any errors", t, func() {
		Convey("when the version has all fields", func() {
			b, err := json.Marshal(associatedVersion)
			if err != nil {
				log.ErrorC("Failed to marshal test data into bytes", err, nil)
				os.Exit(1)
			}
			r := bytes.NewReader(b)
			version, err := CreateVersion(r)
			So(err, ShouldBeNil)
			So(version.CollectionID, ShouldEqual, collectionID)
			So(version.Dimensions, ShouldResemble, []CodeList{dimension})
			So(version.Downloads, ShouldResemble, &downloads)
			So(version.Edition, ShouldEqual, "2017")
			So(version.ID, ShouldNotBeNil)
			So(version.ReleaseDate, ShouldEqual, "2017-10-12")
			So(version.Links.Spatial.HRef, ShouldEqual, "http://ons.gov.uk/geographylist")
			So(version.State, ShouldEqual, "associated")
			So(version.Temporal, ShouldResemble, &[]TemporalFrequency{temporal})
			So(version.Version, ShouldEqual, 1)
		})
	})

	Convey("Return with error when the request body contains the correct fields but of the wrong type", t, func() {
		b, err := json.Marshal(badInputData)
		if err != nil {
			log.ErrorC("Failed to marshal test data into bytes", err, nil)
			os.Exit(1)
		}
		r := bytes.NewReader(b)
		version, err := CreateVersion(r)
		So(version, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(err, ShouldResemble, errors.New("Failed to parse json body"))
	})
}

func TestValidateVersion(t *testing.T) {
	t.Parallel()
	Convey("Successfully return without any errors", t, func() {
		Convey("when the version state is created", func() {

			err := ValidateVersion(&createdVersion)
			So(err, ShouldBeNil)
		})

		Convey("when the version state is associated", func() {

			err := ValidateVersion(&associatedVersion)
			So(err, ShouldBeNil)
		})

		Convey("when the version state is published", func() {

			err := ValidateVersion(&publishedVersion)
			So(err, ShouldBeNil)
		})
	})

	Convey("Return with errors", t, func() {
		Convey("when the version state is set to an invalid value", func() {

			err := ValidateVersion(&Version{State: "submitted"})
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errors.New("Incorrect state, can be one of the following: edition-confirmed, associated or published"))
		})

		Convey("when mandatorey fields are missing from version document when state is set to created", func() {

			err := ValidateVersion(&Version{State: "edition-confirmed"})
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errors.New("Missing mandatory fields: [release_date]"))
		})

		Convey("when the version state is published but is missing collection_id", func() {
			version := &Version{
				ReleaseDate: "2016-04-04",
				State:       "published",
			}

			err := ValidateVersion(version)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errors.New("Missing collection_id for association between version and a collection"))
		})
	})
}
