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

var contacts = ContactDetails{
	Email:     "test@test.co.uk",
	Name:      "john test",
	Telephone: "01654 765432",
}

var methodology = GeneralDetails{
	Description: "some methodology description",
	HRef:        "http://localhost:22000//datasets/methodologies",
	Title:       "some methodology title",
}

var publications = GeneralDetails{
	Description: "some publication description",
	HRef:        "http://localhost:22000//datasets/publications",
	Title:       "some publication title",
}

var qmi = GeneralDetails{}

var relatedDatasets = GeneralDetails{
	HRef:  "http://localhost:22000//datasets/124",
	Title: "Census Age",
}

var inputDataset = &Dataset{
	CollectionID: "12345678",
	Contacts: []ContactDetails{
		contacts,
	},
	Description:       "census",
	Keywords:          []string{"test", "test2"},
	NationalStatistic: true,
	Methodologies: []GeneralDetails{
		methodology,
	},
	NextRelease: "2016-05-05",
	Publications: []GeneralDetails{
		publications,
	},
	Publisher: Publisher{
		Name: "The office of national statistics",
		Type: "government",
		HRef: "https://www.ons.gov.uk/",
	},
	QMI: GeneralDetails{
		Description: "some qmi description",
		HRef:        "http://localhost:22000//datasets/123/qmi",
		Title:       "Quality and Methodology Information",
	},
	RelatedDatasets: []GeneralDetails{
		relatedDatasets,
	},
	ReleaseFrequency: "yearly",
	State:            "published",
	Theme:            "population",
	Title:            "CensusEthnicity",
	URI:              "http://localhost:22000/datasets/123/breadcrumbs",
}

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
			dataset, err := CreateDataset(r)
			So(err, ShouldBeNil)
			So(dataset.CollectionID, ShouldEqual, "12345678")
			So(dataset.Contacts[0].Email, ShouldEqual, "test@test.co.uk")
			So(dataset.Contacts[0].Name, ShouldEqual, "john test")
			So(dataset.Contacts[0].Telephone, ShouldEqual, "01654 765432")
			So(dataset.Description, ShouldEqual, "census")
			So(dataset.ID, ShouldNotBeNil)
			So(dataset.Keywords[0], ShouldEqual, "test")
			So(dataset.Keywords[1], ShouldEqual, "test2")
			So(dataset.Methodologies[0].Description, ShouldEqual, "some methodology description")
			So(dataset.Methodologies[0].HRef, ShouldEqual, "http://localhost:22000//datasets/methodologies")
			So(dataset.Methodologies[0].Title, ShouldEqual, "some methodology title")
			So(dataset.NationalStatistic, ShouldEqual, true)
			So(dataset.NextRelease, ShouldEqual, "2016-05-05")
			So(dataset.Publications[0].Description, ShouldEqual, "some publication description")
			So(dataset.Publications[0].HRef, ShouldEqual, "http://localhost:22000//datasets/publications")
			So(dataset.Publications[0].Title, ShouldEqual, "some publication title")
			So(dataset.Publisher.HRef, ShouldEqual, "https://www.ons.gov.uk/")
			So(dataset.Publisher.Name, ShouldEqual, "The office of national statistics")
			So(dataset.Publisher.Type, ShouldEqual, "government")
			So(dataset.QMI.Description, ShouldEqual, "some qmi description")
			So(dataset.QMI.HRef, ShouldEqual, "http://localhost:22000//datasets/123/qmi")
			So(dataset.QMI.Title, ShouldEqual, "Quality and Methodology Information")
			So(dataset.RelatedDatasets[0].HRef, ShouldEqual, "http://localhost:22000//datasets/124")
			So(dataset.RelatedDatasets[0].Title, ShouldEqual, "Census Age")
			So(dataset.ReleaseFrequency, ShouldEqual, "yearly")
			So(dataset.State, ShouldEqual, "created")
			So(dataset.Theme, ShouldEqual, "population")
			So(dataset.Title, ShouldEqual, "CensusEthnicity")
			So(dataset.URI, ShouldEqual, "http://localhost:22000/datasets/123/breadcrumbs")
		})
	})

	Convey("Return with errors", t, func() {
		Convey("when the version state is set to an invalid value", func() {
			version := &Version{
				State: "submitted",
			}

			err := ValidateVersion(version)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errors.New("Incorrect state, can be one of the following: created, associated or published"))
		})

		Convey("when mandatorey fields are missing from version document when state is set to created", func() {
			version := &Version{
				State: "created",
			}

			err := ValidateVersion(version)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errors.New("Missing mandatory fields: [instance_id license release_date]"))
		})

		Convey("when the version state is published but is missing collection_id", func() {
			version := &Version{
				InstanceID:  "12345678",
				License:     "ONS License",
				ReleaseDate: "2016-04-04",
				State:       "published",
			}

			err := ValidateVersion(version)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errors.New("Missing collection_id for association between version and a collection"))
		})
	})
}

func TestValidateVersion(t *testing.T) {
	t.Parallel()
	Convey("Successfully return without any errors", t, func() {
		Convey("when the version state is created", func() {
			version := &Version{
				InstanceID:  "12345678",
				License:     "ONS License",
				ReleaseDate: "2016-04-04",
				State:       "created",
			}

			err := ValidateVersion(version)
			So(err, ShouldBeNil)
		})

		Convey("when the version state is associated", func() {
			version := &Version{
				CollectionID: "87654321",
				InstanceID:   "12345678",
				License:      "ONS License",
				ReleaseDate:  "2016-04-04",
				State:        "associated",
			}

			err := ValidateVersion(version)
			So(err, ShouldBeNil)
		})

		Convey("when the version state is published", func() {
			version := &Version{
				CollectionID: "87654321",
				InstanceID:   "12345678",
				License:      "ONS License",
				ReleaseDate:  "2016-04-04",
				State:        "published",
			}

			err := ValidateVersion(version)
			So(err, ShouldBeNil)
		})
	})

	Convey("Return with errors", t, func() {
		Convey("when the version state is set to an invalid value", func() {
			version := &Version{
				State: "submitted",
			}

			err := ValidateVersion(version)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errors.New("Incorrect state, can be one of the following: created, associated or published"))
		})

		Convey("when mandatorey fields are missing from version document when state is set to created", func() {
			version := &Version{
				State: "created",
			}

			err := ValidateVersion(version)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errors.New("Missing mandatory fields: [instance_id license release_date]"))
		})

		Convey("when the version state is published but is missing collection_id", func() {
			version := &Version{
				InstanceID:  "12345678",
				License:     "ONS License",
				ReleaseDate: "2016-04-04",
				State:       "published",
			}

			err := ValidateVersion(version)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errors.New("Missing collection_id for association between version and a collection"))
		})
	})
}
