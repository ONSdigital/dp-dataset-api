package models

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/pkg/errors"
	"github.com/smartystreets/goconvey/convey"
)

const (
	validURI    = "http://localhost:22000/datasets/123"
	validHref   = "http://localhost:22000/datasets/href"
	invalidHref = ":invalid"
)

func createDataset() Dataset {
	return Dataset{
		ID:  "123",
		URI: validURI,
		QMI: &GeneralDetails{
			Description: "some qmi description",
			HRef:        validHref,
			Title:       "some qmi title",
		},
		Publisher: &Publisher{
			HRef: validHref,
		},
		Publications: []GeneralDetails{{
			Description: "some publication description",
			HRef:        validHref,
			Title:       "some publication title",
		}},
		Methodologies: []GeneralDetails{{
			Description: "some methodologies description",
			HRef:        validHref,
			Title:       "some publication title",
		}},
		RelatedDatasets: []GeneralDetails{{
			Description: "some related datasets description",
			HRef:        validHref,
			Title:       "some publication title",
		}},
	}
}

var testContext = context.Background()

func TestString(t *testing.T) {
	convey.Convey("Given an index for a dataset type", t, func() {
		convey.Convey("Then it should return the appropriate value", func() {
			result := Filterable.String()
			convey.So(result, convey.ShouldEqual, "filterable")
			convey.So(datasetTypes[0], convey.ShouldEqual, "filterable")
			convey.So(datasetTypes[1], convey.ShouldEqual, "cantabular_table")
			convey.So(datasetTypes[2], convey.ShouldEqual, "cantabular_blob")
			convey.So(datasetTypes[3], convey.ShouldEqual, "cantabular_flexible_table")
			convey.So(datasetTypes[4], convey.ShouldEqual, "cantabular_multivariate_table")
			convey.So(datasetTypes[5], convey.ShouldEqual, "static")
			convey.So(datasetTypes[6], convey.ShouldEqual, "invalid")
		})
	})
}

func TestGetDatasetType(t *testing.T) {
	convey.Convey("Given the dataset type", t, func() {
		convey.Convey("When the type is empty", func() {
			convey.Convey("Then it should default to filterable", func() {
				result, err := GetDatasetType("")
				convey.So(result, convey.ShouldEqual, Filterable)
				convey.So(err, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the type is cantabular_blob", func() {
			convey.Convey("Then it should return the appropriate value", func() {
				result, err := GetDatasetType("cantabular_blob")
				convey.So(result, convey.ShouldEqual, CantabularBlob)
				convey.So(err, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the type is cantabular_table", func() {
			convey.Convey("Then it should return the appropriate value", func() {
				result, err := GetDatasetType("cantabular_table")
				convey.So(result, convey.ShouldEqual, CantabularTable)
				convey.So(err, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the type is cantabular_flexible_table", func() {
			convey.Convey("Then it should return the appropriate value", func() {
				result, err := GetDatasetType("cantabular_flexible_table")
				convey.So(result, convey.ShouldEqual, CantabularFlexibleTable)
				convey.So(err, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the type is static", func() {
			convey.Convey("Then it should return the appropriate value", func() {
				result, err := GetDatasetType("static")
				convey.So(result, convey.ShouldEqual, Static)
				convey.So(err, convey.ShouldBeNil)
			})
		})

		convey.Convey("When the type is invalid", func() {
			convey.Convey("Then an error should be returned", func() {
				result, err := GetDatasetType("abcdefg")
				convey.So(result, convey.ShouldEqual, Invalid)
				convey.So(err, convey.ShouldResemble, errs.ErrDatasetTypeInvalid)
			})
		})
	})
}

func TestValidateDatasetType(t *testing.T) {
	convey.Convey("Given a dataset type return an error ", t, func() {
		convey.Convey("When the request has invalid dataset type ", func() {
			convey.Convey("Then should return type invalid error", func() {
				dt, err := ValidateDatasetType(testContext, "abc123")
				convey.So(dt, convey.ShouldBeNil)
				convey.So(err, convey.ShouldResemble, errs.ErrDatasetTypeInvalid)
			})
		})
	})
}

func TestCreateDataset(t *testing.T) {
	t.Parallel()

	convey.Convey("Successfully return without any errors", t, func() {
		convey.Convey("when the dataset has all fields for PUT request", func() {
			inputDataset := createTestDataset()

			b, err := json.Marshal(inputDataset)
			if err != nil {
				t.Logf("failed to marshal test data into bytes, error: %v", err)
				t.FailNow()
			}
			r := bytes.NewReader(b)
			dataset, err := CreateDataset(r)
			convey.So(err, convey.ShouldBeNil)
			convey.So(dataset.Links.AccessRights.HRef, convey.ShouldEqual, "http://ons.gov.uk/accessrights")
			convey.So(dataset.CollectionID, convey.ShouldEqual, collectionID)
			convey.So(dataset.Contacts[0], convey.ShouldResemble, contacts)
			convey.So(dataset.Description, convey.ShouldEqual, "census")
			convey.So(dataset.ID, convey.ShouldNotBeNil)
			convey.So(dataset.Keywords[0], convey.ShouldEqual, "test")
			convey.So(dataset.Keywords[1], convey.ShouldEqual, "test2")
			convey.So(dataset.License, convey.ShouldEqual, "Office of National Statistics license")
			convey.So(dataset.Methodologies[0], convey.ShouldResemble, methodology)
			convey.So(dataset.NationalStatistic, convey.ShouldResemble, &nationalStatistic)
			convey.So(dataset.NextRelease, convey.ShouldEqual, "2016-05-05")
			convey.So(dataset.Publications[0], convey.ShouldResemble, publications)
			convey.So(dataset.Publisher, convey.ShouldResemble, &publisher)
			convey.So(dataset.QMI, convey.ShouldResemble, &qmi)
			convey.So(dataset.RelatedDatasets[0], convey.ShouldResemble, relatedDatasets)
			convey.So(dataset.ReleaseFrequency, convey.ShouldEqual, "yearly")
			convey.So(dataset.State, convey.ShouldEqual, AssociatedState)
			convey.So(dataset.Theme, convey.ShouldEqual, "population")
			convey.So(dataset.Title, convey.ShouldEqual, "CensusEthnicity")
			convey.So(dataset.UnitOfMeasure, convey.ShouldEqual, "Pounds Sterling")
			convey.So(dataset.URI, convey.ShouldEqual, "http://localhost:22000/datasets/123/breadcrumbs")
			convey.So(dataset.Type, convey.ShouldEqual, "filterable")
			convey.So(dataset.CanonicalTopic, convey.ShouldResemble, canonicalTopic)
			convey.So(dataset.Subtopics[0], convey.ShouldResemble, subtopic)
			convey.So(dataset.Survey, convey.ShouldEqual, survey)
			convey.So(dataset.RelatedContent, convey.ShouldResemble, relatedContent)
		})
	})

	convey.Convey("Successfully return without any errors", t, func() {
		convey.Convey("when the dataset has all fields for PUT request", func() {
			inputDataset := createTestDataset()
			expectedDataset := expectedDataset()

			b, err := json.Marshal(inputDataset)
			if err != nil {
				t.Logf("failed to marshal test data into bytes, error: %v", err)
				t.FailNow()
			}
			r := bytes.NewReader(b)
			dataset, err := CreateDataset(r)
			convey.So(dataset.ID, convey.ShouldNotBeNil)

			// Check id exists and emove before comparison with expected dataset; id
			// is generated each time CreateDataset is called
			convey.So(err, convey.ShouldBeNil)
			dataset.ID = ""

			convey.So(dataset, convey.ShouldResemble, &expectedDataset)
		})
	})

	convey.Convey("Return with error when the request body contains the correct fields but of the wrong type", t, func() {
		b, err := json.Marshal(badInputData)
		if err != nil {
			t.Logf("failed to marshal test data into bytes, error: %v", err)
			t.FailNow()
		}
		r := bytes.NewReader(b)
		version, err := CreateDataset(r)
		convey.So(version, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldResemble, errs.ErrUnableToParseJSON)
	})
}

func TestCreateVersion(t *testing.T) {
	t.Parallel()
	convey.Convey("Successfully return without any errors", t, func() {
		convey.Convey("when the version has all fields", func() {
			testDatasetID := "test-dataset-id"
			b, err := json.Marshal(associatedVersion)
			if err != nil {
				t.Logf("failed to marshal test data into bytes, error: %v", err)
				t.FailNow()
			}
			r := bytes.NewReader(b)
			version, err := CreateVersion(r, testDatasetID)
			convey.So(err, convey.ShouldBeNil)
			convey.So(version.CollectionID, convey.ShouldEqual, collectionID)
			convey.So(version.Dimensions, convey.ShouldResemble, []Dimension{dimension})
			convey.So(version.DatasetID, convey.ShouldEqual, testDatasetID)
			convey.So(version.Downloads, convey.ShouldResemble, &downloads)
			convey.So(version.Edition, convey.ShouldEqual, "2017")
			convey.So(version.ID, convey.ShouldNotBeNil)
			convey.So(version.ReleaseDate, convey.ShouldEqual, "2017-10-12")
			convey.So(version.LatestChanges, convey.ShouldResemble, &[]LatestChange{latestChange})
			convey.So(version.Links.Spatial.HRef, convey.ShouldEqual, "http://ons.gov.uk/geographylist")
			convey.So(version.State, convey.ShouldEqual, AssociatedState)
			convey.So(version.Temporal, convey.ShouldResemble, &[]TemporalFrequency{temporal})
			convey.So(version.Version, convey.ShouldEqual, 1)
		})
	})

	convey.Convey("Return with error when the request body contains the correct fields but of the wrong type", t, func() {
		testDatasetID := "test-dataset-id"
		b, err := json.Marshal(badInputData)
		if err != nil {
			t.Logf("failed to marshal test data into bytes, error: %v", err)
			t.FailNow()
		}
		r := bytes.NewReader(b)
		version, err := CreateVersion(r, testDatasetID)
		convey.So(version, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldResemble, errs.ErrUnableToParseJSON)
	})
}

func TestCleanDataset(t *testing.T) {
	t.Parallel()

	convey.Convey("A clean dataset stays unmodified", t, func() {
		convey.Convey("When a clean dataset is cleaned, the URI and hrefs stay the same", func() {
			dataset := createDataset()
			CleanDataset(&dataset)
			convey.So(dataset.URI, convey.ShouldEqual, validURI)
			convey.So(dataset.Publications, convey.ShouldHaveLength, 1)
			convey.So(dataset.Publications[0].HRef, convey.ShouldEqual, validHref)
		})
	})

	convey.Convey("A dirty dataset is cleaned", t, func() {
		convey.Convey("When a dataset URI has leading space it is trimmed", func() {
			dataset := createDataset()
			dataset.URI = "    " + validURI
			CleanDataset(&dataset)
			convey.So(dataset.URI, convey.ShouldEqual, validURI)
		})

		convey.Convey("When a dataset URI has trailing space it is trimmed", func() {
			dataset := createDataset()
			dataset.URI = validURI + "     "
			CleanDataset(&dataset)
			convey.So(dataset.URI, convey.ShouldEqual, validURI)
		})

		convey.Convey("When a QMI HRef has whitespace it is trimmed", func() {
			dataset := createDataset()
			dataset.QMI.HRef = "    " + validHref
			CleanDataset(&dataset)
			convey.So(dataset.QMI.HRef, convey.ShouldEqual, validHref)
		})

		convey.Convey("When a Publisher HRef has whitespace it is trimmed", func() {
			dataset := createDataset()
			dataset.Publisher.HRef = "    " + validHref
			CleanDataset(&dataset)
			convey.So(dataset.Publisher.HRef, convey.ShouldEqual, validHref)
		})

		convey.Convey("When a Publications HRef has whitespace it is trimmed", func() {
			dataset := createDataset()
			dataset.Publications[0].HRef = "    " + validHref
			CleanDataset(&dataset)
			convey.So(dataset.Publications, convey.ShouldHaveLength, 1)
			convey.So(dataset.Publications[0].HRef, convey.ShouldEqual, validHref)
		})

		convey.Convey("When two Publications HRef's have whitespace they are trimmed", func() {
			dataset := createDataset()
			dataset.Publications[0].HRef = "    " + validHref
			dataset.Publications = append(dataset.Publications, GeneralDetails{HRef: validHref + "    "})
			CleanDataset(&dataset)
			convey.So(dataset.Publications, convey.ShouldHaveLength, 2)
			convey.So(dataset.Publications[0].HRef, convey.ShouldEqual, validHref)
			convey.So(dataset.Publications[1].HRef, convey.ShouldEqual, validHref)
		})

		convey.Convey("When a Methodologies HRef has whitespace it is trimmed", func() {
			dataset := createDataset()
			dataset.Methodologies[0].HRef = "    " + validHref
			CleanDataset(&dataset)
			convey.So(dataset.Methodologies, convey.ShouldHaveLength, 1)
			convey.So(dataset.Methodologies[0].HRef, convey.ShouldEqual, validHref)
		})

		convey.Convey("When two Methodologies HRef's have whitespace they are trimmed", func() {
			dataset := createDataset()
			dataset.Methodologies[0].HRef = "    " + validHref
			dataset.Methodologies = append(dataset.Methodologies, GeneralDetails{HRef: validHref + "    "})
			CleanDataset(&dataset)
			convey.So(dataset.Methodologies, convey.ShouldHaveLength, 2)
			convey.So(dataset.Methodologies[0].HRef, convey.ShouldEqual, validHref)
			convey.So(dataset.Methodologies[1].HRef, convey.ShouldEqual, validHref)
		})

		convey.Convey("When a RelatedDatasets HRef has whitespace it is trimmed", func() {
			dataset := createDataset()
			dataset.RelatedDatasets[0].HRef = "    " + validHref
			CleanDataset(&dataset)
			convey.So(dataset.RelatedDatasets, convey.ShouldHaveLength, 1)
			convey.So(dataset.RelatedDatasets[0].HRef, convey.ShouldEqual, validHref)
		})

		convey.Convey("When two RelatedDatasets HRef's have whitespace they are trimmed", func() {
			dataset := createDataset()
			dataset.RelatedDatasets[0].HRef = "    " + validHref
			dataset.RelatedDatasets = append(dataset.RelatedDatasets, GeneralDetails{HRef: validHref + "    "})
			CleanDataset(&dataset)
			convey.So(dataset.RelatedDatasets, convey.ShouldHaveLength, 2)
			convey.So(dataset.RelatedDatasets[0].HRef, convey.ShouldEqual, validHref)
			convey.So(dataset.RelatedDatasets[1].HRef, convey.ShouldEqual, validHref)
		})
	})
}

func TestValidateDataset(t *testing.T) {
	t.Parallel()

	convey.Convey("Successful validation (true) returned", t, func() {
		convey.Convey("when dataset.URI contains its path in appropriate url format", func() {
			dataset := createDataset()
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldBeNil)
		})

		convey.Convey("when dataset.URI is empty", func() {
			dataset := createDataset()
			dataset.URI = ""
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldBeNil)
		})

		convey.Convey("when dataset.URI is a relative path", func() {
			dataset := createDataset()
			dataset.URI = "/relative_path"
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldBeNil)
		})

		convey.Convey("when dataset.URI has a valid host but an empty path", func() {
			dataset := createDataset()
			dataset.URI = "http://domain.com/"
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldBeNil)
		})

		convey.Convey("when dataset.URI is only a valid domain", func() {
			dataset := createDataset()
			dataset.URI = "domain.com"
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldBeNil)
		})
	})

	convey.Convey("Unsuccessful validation (false) returned", t, func() {
		convey.Convey("when dataset.URI is unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.URI = ":foo"
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldNotBeNil)
			convey.So(validationErr.Error(), convey.ShouldResemble, errors.New("invalid fields: [URI]").Error())
		})

		convey.Convey("when dataset.URI has an empty host and path", func() {
			dataset := createDataset()
			dataset.URI = "http://"
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldNotBeNil)
			convey.So(validationErr.Error(), convey.ShouldResemble, errors.New("invalid fields: [URI]").Error())
		})

		convey.Convey("when dataset.URI has an empty host but a non empty path", func() {
			dataset := createDataset()
			dataset.URI = "http:///path"
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldNotBeNil)
			convey.So(validationErr.Error(), convey.ShouldResemble, errors.New("invalid fields: [URI]").Error())
		})

		convey.Convey("when dataset.QMI.Href is unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.QMI.HRef = ":foo"
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldNotBeNil)
			convey.So(validationErr.Error(), convey.ShouldResemble, errors.New("invalid fields: [QMI]").Error())
		})

		convey.Convey("when dataset.Publisher.Href is unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.Publisher.HRef = ":foo"
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldNotBeNil)
			convey.So(validationErr.Error(), convey.ShouldResemble, errors.New("invalid fields: [Publisher]").Error())
		})

		convey.Convey("when Publications href is unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.Publications[0].HRef = invalidHref
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldNotBeNil)
			convey.So(validationErr.Error(), convey.ShouldResemble, errors.New("invalid fields: [Publications[0].HRef]").Error())
		})

		convey.Convey("when Methodologies href is unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.Methodologies[0].HRef = invalidHref
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldNotBeNil)
			convey.So(validationErr.Error(), convey.ShouldResemble, errors.New("invalid fields: [Methodologies[0].HRef]").Error())
		})

		convey.Convey("when RelatedDatasets href is unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.RelatedDatasets[0].HRef = invalidHref
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldNotBeNil)
			convey.So(validationErr.Error(), convey.ShouldResemble, errors.New("invalid fields: [RelatedDatasets[0].HRef]").Error())
		})

		convey.Convey("when all href and URI fields are unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.URI = invalidHref
			dataset.Publications[0].HRef = invalidHref
			dataset.Methodologies[0].HRef = invalidHref
			dataset.RelatedDatasets[0].HRef = invalidHref
			validationErr := ValidateDataset(&dataset)
			convey.So(validationErr, convey.ShouldNotBeNil)
			convey.So(validationErr.Error(), convey.ShouldResemble, errors.New("invalid fields: [URI Publications[0].HRef RelatedDatasets[0].HRef Methodologies[0].HRef]").Error())
		})
	})
}

func TestValidateVersion(t *testing.T) {
	t.Parallel()
	convey.Convey("Successfully return without any errors", t, func() {
		convey.Convey("when the version state is created", func() {
			err := ValidateVersion(&editionConfirmedVersion)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when the version state is associated", func() {
			err := ValidateVersion(&associatedVersion)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when the version state is published", func() {
			err := ValidateVersion(&publishedVersion)
			convey.So(err, convey.ShouldBeNil)
		})
	})

	convey.Convey("Return with errors", t, func() {
		convey.Convey("when the version state is empty", func() {
			err := ValidateVersion(&Version{State: ""})
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldResemble, errors.New("missing state from version").Error())
		})

		convey.Convey("when the version state is set to an invalid value", func() {
			err := ValidateVersion(&Version{State: SubmittedState})
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldResemble, errors.New("incorrect state, can be one of the following: edition-confirmed, associated or published").Error())
		})

		convey.Convey("when mandatory fields are missing from version document when state is set to created", func() {
			err := ValidateVersion(&Version{State: EditionConfirmedState})
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldResemble, errors.New("missing mandatory fields: [release_date]").Error())
		})

		convey.Convey("when the version state is published but has a collection_id", func() {
			version := &Version{
				ReleaseDate:  "2016-04-04",
				State:        PublishedState,
				CollectionID: "cid01",
			}

			err := ValidateVersion(version)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldResemble, errors.New("unexpected collection_id in published version").Error())
		})

		convey.Convey("when version downloads are invalid", func() {
			v := &Version{ReleaseDate: "Today", State: EditionConfirmedState}

			v.Downloads = &DownloadList{XLS: &DownloadObject{HRef: "", Size: "2"}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.XLS.HRef"}), v)

			v.Downloads = &DownloadList{XLSX: &DownloadObject{HRef: "", Size: "2"}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.XLSX.HRef"}), v)

			v.Downloads = &DownloadList{CSV: &DownloadObject{HRef: "", Size: "2"}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.CSV.HRef"}), v)

			v.Downloads = &DownloadList{CSVW: &DownloadObject{HRef: "", Size: "2"}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.CSVW.HRef"}), v)

			v.Downloads = &DownloadList{TXT: &DownloadObject{HRef: "", Size: "2"}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.TXT.HRef"}), v)

			v.Downloads = &DownloadList{XLS: &DownloadObject{HRef: "/", Size: ""}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.XLS.Size"}), v)

			v.Downloads = &DownloadList{XLSX: &DownloadObject{HRef: "/", Size: ""}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.XLSX.Size"}), v)

			v.Downloads = &DownloadList{CSV: &DownloadObject{HRef: "/", Size: ""}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.CSV.Size"}), v)

			v.Downloads = &DownloadList{CSVW: &DownloadObject{HRef: "/", Size: ""}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.CSVW.Size"}), v)

			v.Downloads = &DownloadList{TXT: &DownloadObject{HRef: "/", Size: ""}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.TXT.Size"}), v)

			v.Downloads = &DownloadList{XLS: &DownloadObject{HRef: "/", Size: "bob"}}
			assertVersionDownloadError(fmt.Errorf("invalid fields: %v", []string{"Downloads.XLS.Size not a number"}), v)

			v.Downloads = &DownloadList{XLSX: &DownloadObject{HRef: "/", Size: "bob"}}
			assertVersionDownloadError(fmt.Errorf("invalid fields: %v", []string{"Downloads.XLSX.Size not a number"}), v)

			v.Downloads = &DownloadList{CSV: &DownloadObject{HRef: "/", Size: "bob"}}
			assertVersionDownloadError(fmt.Errorf("invalid fields: %v", []string{"Downloads.CSV.Size not a number"}), v)

			v.Downloads = &DownloadList{CSVW: &DownloadObject{HRef: "/", Size: "bob"}}
			assertVersionDownloadError(fmt.Errorf("invalid fields: %v", []string{"Downloads.CSVW.Size not a number"}), v)

			v.Downloads = &DownloadList{TXT: &DownloadObject{HRef: "/", Size: "bob"}}
			assertVersionDownloadError(fmt.Errorf("invalid fields: %v", []string{"Downloads.TXT.Size not a number"}), v)
		})
	})
}

func TestVersionHash(t *testing.T) {
	testVersion := func() Version {
		return Version{
			Alerts: &[]Alert{
				{
					Date:        "today",
					Description: "some error happened",
					Type:        "alertingAlert",
				},
			},
			CollectionID: "testCollection",
			ID:           "myVersion",
			Edition:      "myEdition",
			State:        CreatedState,
			Version:      1,
			Dimensions: []Dimension{
				{
					HRef: "http://dimensions.co.uk/dim1",
					Name: "dim1",
				},
				{
					HRef: "http://dimensions.co.uk/dim2",
					Name: "dim2",
				},
			},
			Downloads: &DownloadList{
				CSV: &DownloadObject{
					Private: "private/link.csv",
					HRef:    "downloadservice/link.csv",
				},
			},
		}
	}

	convey.Convey("Given a version with some data", t, func() {
		v := testVersion()

		convey.Convey("We can generate a valid hash", func() {
			h, err := v.Hash(nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(len(h), convey.ShouldEqual, 40)

			convey.Convey("Then hashing it twice, produces the same result", func() {
				hash, err := v.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldEqual, h)
			})

			convey.Convey("Then storing the hash as its ETag value and hashing it again, produces the same result (field is ignored) and ETag field is preserved", func() {
				v.ETag = h
				hash, err := v.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldEqual, h)
				convey.So(v.ETag, convey.ShouldEqual, h)
			})

			convey.Convey("Then another version with exactly the same data will resolve to the same hash", func() {
				v2 := testVersion()
				hash, err := v2.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldEqual, h)
			})

			convey.Convey("Then if a version value is modified, its hash changes", func() {
				v.State = CompletedState
				hash, err := v.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldNotEqual, h)
			})

			convey.Convey("Then if a download link is added to the version, its hash changes", func() {
				v.Downloads.TXT = &DownloadObject{
					Private: "private/link.txt",
					HRef:    "downloadservice/link.txt",
				}
				hash, err := v.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldNotEqual, h)
			})

			convey.Convey("Then if a dimension is removed from the version, its hash changes", func() {
				v.Dimensions = []Dimension{
					{
						HRef: "http://dimensions.co.uk/dim1",
						Name: "dim1",
					},
				}
				hash, err := v.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldNotEqual, h)
			})
		})
	})
}

func assertVersionDownloadError(expected error, v *Version) {
	err := ValidateVersion(v)
	convey.So(err, convey.ShouldNotBeNil)
	convey.So(err, convey.ShouldResemble, expected)
}

func TestCreateDownloadList(t *testing.T) {
	convey.Convey("invalid input bytes return the expected error", t, func() {
		reader := bytes.NewReader([]byte("hello"))
		dl, err := CreateDownloadList(reader)
		convey.So(dl, convey.ShouldBeNil)
		convey.So(reflect.TypeOf(errors.Cause(err)), convey.ShouldEqual, reflect.TypeOf(&json.SyntaxError{}))
	})

	convey.Convey("valid input returns the expected value", t, func() {
		expected := &DownloadList{
			XLS: &DownloadObject{
				Size: "1",
				HRef: "2",
			},
		}

		input, _ := json.Marshal(expected)
		reader := bytes.NewReader(input)

		dl, err := CreateDownloadList(reader)
		convey.So(err, convey.ShouldBeNil)
		convey.So(dl, convey.ShouldResemble, expected)
	})
}

func TestUpdateLinks(t *testing.T) {
	host := "example.com"

	convey.Convey("Given a new edition with no links", t, func() {
		edition := &EditionUpdate{
			ID: "test",
			Next: &Edition{
				ID:      "test",
				Edition: "time-series",
			},
		}

		convey.Convey("when UpdateLinks is called", func() {
			err := edition.UpdateLinks(testContext, host)

			convey.Convey("then an error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldEqual, "editions links do not exist")
			})
		})
	})

	convey.Convey("Given an edition with only unpublished versions ", t, func() {
		edition := &EditionUpdate{
			ID: "test",
			Next: &Edition{
				ID:      "test",
				Edition: "time-series",
				Links: &EditionUpdateLinks{
					LatestVersion: &LinkObject{
						ID:   "1",
						HRef: "example.com/datasets/1/editions/time-series/versions/1",
					},
					Dataset: &LinkObject{
						ID:   "1",
						HRef: "example.com/datasets/1",
					},
					Self: &LinkObject{
						HRef: "example.com/datasets/1/editions/time-series",
					},
				},
			},
		}

		convey.Convey("when UpdateLinks is called", func() {
			err := edition.UpdateLinks(testContext, host)

			convey.Convey("then links are correctly updated", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(edition.Next.Links.LatestVersion.ID, convey.ShouldEqual, "2")
				convey.So(edition.Current, convey.ShouldBeNil)
			})
		})
	})

	convey.Convey("Given an edition with a published version ", t, func() {
		edition := &EditionUpdate{
			ID: "test",
			Next: &Edition{
				ID:      "test",
				Edition: "time-series",
				Links: &EditionUpdateLinks{
					LatestVersion: &LinkObject{
						ID:   "1",
						HRef: "example.com/datasets/1/editions/time-series/versions/1",
					},
					Dataset: &LinkObject{
						ID:   "1",
						HRef: "example.com/datasets/1",
					},
					Self: &LinkObject{
						HRef: "example.com/datasets/1/editions/time-series",
					},
				},
			},
			Current: &Edition{
				ID:      "test",
				Edition: "time-series",
				Links: &EditionUpdateLinks{
					LatestVersion: &LinkObject{
						ID:   "1",
						HRef: "example.com/datasets/1/editions/time-series/versions/1",
					},
					Dataset: &LinkObject{
						ID:   "1",
						HRef: "example.com/datasets/1",
					},
					Self: &LinkObject{
						HRef: "example.com/datasets/1/editions/time-series",
					},
				},
			},
		}

		convey.Convey("when UpdateLinks is called", func() {
			err := edition.UpdateLinks(testContext, host)
			convey.Convey("then links are correctly updated", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(edition.Next.Links.LatestVersion.ID, convey.ShouldEqual, "2")
				convey.So(edition.Current.Links.LatestVersion.ID, convey.ShouldEqual, "1")
			})
		})

		convey.Convey("when UpdateLinks is called with a version ID which is lower than the latest published version", func() {
			edition.Current.Links.LatestVersion.ID = "3"
			err := edition.UpdateLinks(testContext, host)
			convey.Convey("then an error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldResemble, "published edition links to a higher version than the requested change")
			})
		})

		convey.Convey("when UpdateLinks is called on an edition with an invalid current version ID", func() {
			edition.Current.Links.LatestVersion.ID = "hi"
			err := edition.UpdateLinks(testContext, host)
			convey.Convey("then an error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldResemble, "failed to convert version id from edition.current document: strconv.Atoi: parsing \"hi\": invalid syntax")
			})
		})

		convey.Convey("when UpdateLinks is called on an edition with an invalid next version ID", func() {
			edition.Next.Links.LatestVersion.ID = "there"
			err := edition.UpdateLinks(testContext, host)
			convey.Convey("then an error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldResemble, "failed to convert version id from edition.next document: strconv.Atoi: parsing \"there\": invalid syntax")
			})
		})
	})
}

func TestPublishLinks(t *testing.T) {
	convey.Convey("Given a new edition with no links", t, func() {
		edition := &EditionUpdate{
			ID: "test",
			Next: &Edition{
				ID:      "test",
				Edition: "time-series",
			},
		}

		convey.Convey("when PublishLinks is called", func() {
			err := edition.PublishLinks(testContext, nil)

			convey.Convey("then an error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldEqual, "editions links do not exist")
			})
		})
	})

	convey.Convey("Given an edition with an invalid current version ID", t, func() {
		edition := &EditionUpdate{
			ID: "test",
			Next: &Edition{
				ID:      "test",
				Edition: "time-series",
				Links: &EditionUpdateLinks{
					LatestVersion: &LinkObject{
						ID:   "hello",
						HRef: "example.com/datasets/1/editions/time-series/versions/hello",
					},
				},
			},
			Current: &Edition{
				ID:      "test",
				Edition: "time-series",
				Links: &EditionUpdateLinks{
					LatestVersion: &LinkObject{
						ID:   "hello",
						HRef: "example.com/datasets/1/editions/time-series/versions/hello",
					},
				},
			},
		}

		convey.Convey("when PublishLinks is called", func() {
			err := edition.PublishLinks(testContext, nil)

			convey.Convey("then an error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)

				var expError *strconv.NumError
				convey.So(errors.As(err, &expError), convey.ShouldBeTrue)
			})
		})
	})

	convey.Convey("Given an edition with only unpublished versions ", t, func() {
		version := &LinkObject{
			ID:   "1",
			HRef: "example.com/datasets/1/editions/time-series/versions/1",
		}

		edition := &EditionUpdate{
			ID: "test",
			Next: &Edition{
				ID:      "test",
				Edition: "time-series",
				Links: &EditionUpdateLinks{
					LatestVersion: version,
				},
			},
		}

		convey.Convey("when PublishLinks is called with an invalid version link", func() {
			err := edition.PublishLinks(testContext, nil)

			convey.Convey("then an error is returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err.Error(), convey.ShouldResemble, "invalid arguments to PublishLinks - versionLink empty")
			})
		})

		convey.Convey("when PublishLinks is called with an invalid version link ID", func() {
			err := edition.PublishLinks(testContext, &LinkObject{
				ID: "hello",
			})

			convey.Convey("then an error is returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				var expError *strconv.NumError
				convey.So(errors.As(err, &expError), convey.ShouldBeTrue)
			})
		})

		convey.Convey("when PublishLinks is called with a version link", func() {
			err := edition.PublishLinks(testContext, version)

			convey.Convey("then links are correctly updated", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(edition.Next.Links.LatestVersion, convey.ShouldEqual, version)
				convey.So(edition.Current, convey.ShouldBeNil)
			})
		})
	})

	convey.Convey("Given an edition with a published version ", t, func() {
		publishedVersion := &LinkObject{
			ID:   "2",
			HRef: "example.com/datasets/1/editions/time-series/versions/1",
		}

		storedNextVersion := &LinkObject{
			ID:   "2",
			HRef: "example.com/datasets/1/editions/time-series/versions/1",
		}

		edition := &EditionUpdate{
			ID: "test",
			Next: &Edition{
				ID:      "test",
				Edition: "time-series",
				Links: &EditionUpdateLinks{
					LatestVersion: storedNextVersion,
				},
			},
			Current: &Edition{
				ID:      "test",
				Edition: "time-series",
				Links: &EditionUpdateLinks{
					LatestVersion: publishedVersion,
				},
			},
		}

		convey.Convey("when PublishLinks is called", func() {
			argLink := &LinkObject{
				ID:   "3",
				HRef: "example.com/datasets/1/editions/time-series/versions/3",
			}

			err := edition.PublishLinks(testContext, argLink)

			convey.Convey("then links are correctly updated", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(edition.Next.Links.LatestVersion, convey.ShouldEqual, argLink)
				convey.So(edition.Current.Links.LatestVersion, convey.ShouldEqual, publishedVersion)
			})
		})

		convey.Convey("when PublishLinks is called with a version ID which is lower than the latest published version", func() {
			argLink := &LinkObject{
				ID:   "1",
				HRef: "example.com/datasets/1/editions/time-series/versions/1",
			}
			err := edition.PublishLinks(testContext, argLink)

			convey.Convey("then no changes should be made", func() {
				convey.So(err, convey.ShouldBeNil)
				convey.So(edition.Next.Links.LatestVersion, convey.ShouldEqual, storedNextVersion)
				convey.So(edition.Current.Links.LatestVersion, convey.ShouldEqual, publishedVersion)
			})
		})
	})
}

func TestValidateVersionNumberSuccess(t *testing.T) {
	convey.Convey("Given valid version number above 0 in string format", t, func() {
		versionStr := "5"

		convey.Convey("When ParseAndValidateVersionNumber is called", func() {
			versionNumber, err := ParseAndValidateVersionNumber(testContext, versionStr)

			convey.Convey("Then no error should be returned", func() {
				convey.So(err, convey.ShouldBeNil)

				convey.Convey("And version number is converted to integer successfully ", func() {
					convey.So(versionNumber, convey.ShouldEqual, 5)
					convey.So(fmt.Sprintf("%T", versionNumber), convey.ShouldEqual, "int")
				})
			})
		})
	})
}

func TestValidateVersionNumberFailure(t *testing.T) {
	convey.Convey("Given invalid version number in string format", t, func() {
		versionStr := "abc"

		convey.Convey("When ParseAndValidateVersionNumber is called", func() {
			_, err := ParseAndValidateVersionNumber(testContext, versionStr)

			convey.Convey("Then an error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err, convey.ShouldResemble, errs.ErrInvalidVersion)
			})
		})
	})

	convey.Convey("Given version number less than 0 in string format", t, func() {
		versionStr := "-1"

		convey.Convey("When ParseAndValidateVersionNumber is called", func() {
			_, err := ParseAndValidateVersionNumber(testContext, versionStr)

			convey.Convey("Then an error should be returned", func() {
				convey.So(err, convey.ShouldNotBeNil)
				convey.So(err, convey.ShouldResemble, errs.ErrInvalidVersion)
			})
		})
	})
}

func TestVersionLinksDeepCopy(t *testing.T) {
	convey.Convey("Given a fully populated VersionLinks", t, func() {
		vl := &VersionLinks{
			Dataset: &LinkObject{
				ID:   "datasetID",
				HRef: "datasetHRef",
			},
			Dimensions: &LinkObject{
				ID:   "dimensionID",
				HRef: "dimensionHRef",
			},
			Edition: &LinkObject{
				ID:   "editionID",
				HRef: "editionHRef",
			},
			Self: &LinkObject{
				ID:   "selfID",
				HRef: "selfHRef",
			},
			Spatial: &LinkObject{
				ID:   "spatialID",
				HRef: "spatialHRef",
			},
			Version: &LinkObject{
				ID:   "versionID",
				HRef: "versionHRef",
			},
		}

		convey.Convey("Then doing a deep copy of it results in a new fully populated VersionLinks", func() {
			vl2 := vl.DeepCopy()
			convey.So(*vl2, convey.ShouldResemble, VersionLinks{
				Dataset: &LinkObject{
					ID:   "datasetID",
					HRef: "datasetHRef",
				},
				Dimensions: &LinkObject{
					ID:   "dimensionID",
					HRef: "dimensionHRef",
				},
				Edition: &LinkObject{
					ID:   "editionID",
					HRef: "editionHRef",
				},
				Self: &LinkObject{
					ID:   "selfID",
					HRef: "selfHRef",
				},
				Spatial: &LinkObject{
					ID:   "spatialID",
					HRef: "spatialHRef",
				},
				Version: &LinkObject{
					ID:   "versionID",
					HRef: "versionHRef",
				},
			})

			convey.So(vl2, convey.ShouldNotPointTo, vl)
			convey.So(vl2.Dataset, convey.ShouldNotPointTo, vl.Dataset)
			convey.So(vl2.Dimensions, convey.ShouldNotPointTo, vl.Dimensions)
			convey.So(vl2.Edition, convey.ShouldNotPointTo, vl.Edition)
			convey.So(vl2.Self, convey.ShouldNotPointTo, vl.Self)
			convey.So(vl2.Spatial, convey.ShouldNotPointTo, vl.Spatial)
			convey.So(vl2.Version, convey.ShouldNotPointTo, vl.Version)
		})
	})

	convey.Convey("Given an empty VersionLinks", t, func() {
		vl := &VersionLinks{}

		convey.Convey("Then doing a deep copy of it results in a new empty VersionLinks", func() {
			vl2 := vl.DeepCopy()
			convey.So(*vl2, convey.ShouldResemble, VersionLinks{})
			convey.So(vl2, convey.ShouldNotPointTo, vl)
		})
	})
}

func TestVersionDownloadsOrder(t *testing.T) {
	convey.Convey("Given a Downloads struct", t, func() {
		d := DownloadList{
			XLS: &DownloadObject{
				HRef: "XLS",
			},
			XLSX: &DownloadObject{
				HRef: "XLSX",
			},
			CSV: &DownloadObject{
				HRef: "CSV",
			},
			CSVW: &DownloadObject{
				HRef: "CSVW",
			},
			TXT: &DownloadObject{
				HRef: "TXT",
			},
		}

		convey.Convey("When marshalled", func() {
			b, err := json.Marshal(d)
			convey.So(err, convey.ShouldBeNil)
			t.Log(string(b))

			convey.Convey("The downloads should be in the expected order", func() {
				expected := `{"xls":{"href":"XLS"},"xlsx":{"href":"XLSX"},"csv":{"href":"CSV"},"txt":{"href":"TXT"},"csvw":{"href":"CSVW"}}`
				convey.So(string(b), convey.ShouldResemble, expected)
			})
		})
	})
}
