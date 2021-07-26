package models

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
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
	Convey("Given an index for a dataset type", t, func() {
		Convey("Then it should return the appropriate value", func() {
			result := Filterable.String()
			So(result, ShouldEqual, "filterable")
			So(datasetTypes[0], ShouldEqual, "filterable")
			So(datasetTypes[1], ShouldEqual, "nomis")
			So(datasetTypes[2], ShouldEqual, "cantabular_table")
			So(datasetTypes[3], ShouldEqual, "cantabular_blob")
			So(datasetTypes[4], ShouldEqual, "invalid")

		})
	})
}

func TestGetDatasetType(t *testing.T) {
	Convey("Given the dataset type", t, func() {
		Convey("When the type is empty", func() {
			Convey("Then it should default to filterable", func() {
				result, err := GetDatasetType("")
				So(result, ShouldEqual, Filterable)
				So(err, ShouldBeNil)
			})
		})

		Convey("When the type is invalid", func() {
			Convey("Then an error should be returned", func() {
				result, err := GetDatasetType("abcdefg")
				So(result, ShouldEqual, Invalid)
				So(err, ShouldResemble, errs.ErrDatasetTypeInvalid)
			})
		})
	})
}

func TestValidateDatasetType(t *testing.T) {
	Convey("Given a dataset type return an error ", t, func() {
		Convey("When the request has invalid dataset type ", func() {
			Convey("Then should return type invalid error", func() {
				dt, err := ValidateDatasetType(testContext, "abc123")
				So(dt, ShouldBeNil)
				So(err, ShouldResemble, errs.ErrDatasetTypeInvalid)
			})
		})
	})
}
func TestValidateNomisURL(t *testing.T) {
	Convey("Given a nomis URL return an error ", t, func() {
		Convey("When the request has filterable type and a nomis url ", func() {
			Convey("Then should return type mismatch", func() {
				_, err := ValidateNomisURL(testContext, "filterable", "www.nomisweb.co.uk")
				So(err, ShouldResemble, errs.ErrTypeMismatch)
			})
		})
	})
}

func TestCreateDataset(t *testing.T) {
	t.Parallel()

	Convey("Successfully return without any errors", t, func() {

		Convey("when the dataset has all fields for PUT request", func() {

			inputDataset := createTestDataset()

			b, err := json.Marshal(inputDataset)
			if err != nil {
				t.Logf("failed to marshal test data into bytes, error: %v", err)
				t.FailNow()
			}
			r := bytes.NewReader(b)
			dataset, err := CreateDataset(r)
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
			So(dataset.State, ShouldEqual, AssociatedState)
			So(dataset.Theme, ShouldEqual, "population")
			So(dataset.Title, ShouldEqual, "CensusEthnicity")
			So(dataset.UnitOfMeasure, ShouldEqual, "Pounds Sterling")
			So(dataset.URI, ShouldEqual, "http://localhost:22000/datasets/123/breadcrumbs")
			So(dataset.Type, ShouldEqual, "filterable")
			So(dataset.NomisReferenceURL, ShouldEqual, "")
		})
	})

	Convey("Successfully return without any errors", t, func() {

		Convey("when the dataset has all fields for PUT request", func() {

			inputDataset := createTestDataset()
			expectedDataset := expectedDataset()

			b, err := json.Marshal(inputDataset)
			if err != nil {
				t.Logf("failed to marshal test data into bytes, error: %v", err)
				t.FailNow()
			}
			r := bytes.NewReader(b)
			dataset, err := CreateDataset(r)
			So(dataset.ID, ShouldNotBeNil)

			// Check id exists and emove before comparison with expected dataset; id
			// is generated each time CreateDataset is called
			So(err, ShouldBeNil)
			dataset.ID = ""

			So(dataset, ShouldResemble, &expectedDataset)
		})
	})

	Convey("Return with error when the request body contains the correct fields but of the wrong type", t, func() {
		b, err := json.Marshal(badInputData)
		if err != nil {
			t.Logf("failed to marshal test data into bytes, error: %v", err)
			t.FailNow()
		}
		r := bytes.NewReader(b)
		version, err := CreateDataset(r)
		So(version, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(err, ShouldResemble, errs.ErrUnableToParseJSON)
	})
}

func TestCreateVersion(t *testing.T) {
	t.Parallel()
	Convey("Successfully return without any errors", t, func() {
		Convey("when the version has all fields", func() {
			testDatasetID := "test-dataset-id"
			b, err := json.Marshal(associatedVersion)
			if err != nil {
				t.Logf("failed to marshal test data into bytes, error: %v", err)
				t.FailNow()
			}
			r := bytes.NewReader(b)
			version, err := CreateVersion(r, testDatasetID)
			So(err, ShouldBeNil)
			So(version.CollectionID, ShouldEqual, collectionID)
			So(version.Dimensions, ShouldResemble, []Dimension{dimension})
			So(version.DatasetID, ShouldEqual, testDatasetID)
			So(version.Downloads, ShouldResemble, &downloads)
			So(version.Edition, ShouldEqual, "2017")
			So(version.ID, ShouldNotBeNil)
			So(version.ReleaseDate, ShouldEqual, "2017-10-12")
			So(version.LatestChanges, ShouldResemble, &[]LatestChange{latestChange})
			So(version.Links.Spatial.HRef, ShouldEqual, "http://ons.gov.uk/geographylist")
			So(version.State, ShouldEqual, AssociatedState)
			So(version.Temporal, ShouldResemble, &[]TemporalFrequency{temporal})
			So(version.Version, ShouldEqual, 1)
		})
	})

	Convey("Return with error when the request body contains the correct fields but of the wrong type", t, func() {
		testDatasetID := "test-dataset-id"
		b, err := json.Marshal(badInputData)
		if err != nil {
			t.Logf("failed to marshal test data into bytes, error: %v", err)
			t.FailNow()
		}
		r := bytes.NewReader(b)
		version, err := CreateVersion(r, testDatasetID)
		So(version, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(err, ShouldResemble, errs.ErrUnableToParseJSON)
	})
}

func TestCleanDataset(t *testing.T) {
	t.Parallel()

	Convey("A clean dataset stays unmodified", t, func() {
		Convey("When a clean dataset is cleaned, the URI and hrefs stay the same", func() {
			dataset := createDataset()
			CleanDataset(&dataset)
			So(dataset.URI, ShouldEqual, validURI)
			So(dataset.Publications, ShouldHaveLength, 1)
			So(dataset.Publications[0].HRef, ShouldEqual, validHref)
		})
	})

	Convey("A dirty dataset is cleaned", t, func() {
		Convey("When a dataset URI has leading space it is trimmed", func() {
			dataset := createDataset()
			dataset.URI = "    " + validURI
			CleanDataset(&dataset)
			So(dataset.URI, ShouldEqual, validURI)
		})

		Convey("When a dataset URI has trailing space it is trimmed", func() {
			dataset := createDataset()
			dataset.URI = validURI + "     "
			CleanDataset(&dataset)
			So(dataset.URI, ShouldEqual, validURI)
		})

		Convey("When a QMI HRef has whitespace it is trimmed", func() {
			dataset := createDataset()
			dataset.QMI.HRef = "    " + validHref
			CleanDataset(&dataset)
			So(dataset.QMI.HRef, ShouldEqual, validHref)
		})

		Convey("When a Publisher HRef has whitespace it is trimmed", func() {
			dataset := createDataset()
			dataset.Publisher.HRef = "    " + validHref
			CleanDataset(&dataset)
			So(dataset.Publisher.HRef, ShouldEqual, validHref)
		})

		Convey("When a Publications HRef has whitespace it is trimmed", func() {
			dataset := createDataset()
			dataset.Publications[0].HRef = "    " + validHref
			CleanDataset(&dataset)
			So(dataset.Publications, ShouldHaveLength, 1)
			So(dataset.Publications[0].HRef, ShouldEqual, validHref)
		})

		Convey("When two Publications HRef's have whitespace they are trimmed", func() {
			dataset := createDataset()
			dataset.Publications[0].HRef = "    " + validHref
			dataset.Publications = append(dataset.Publications, GeneralDetails{HRef: validHref + "    "})
			CleanDataset(&dataset)
			So(dataset.Publications, ShouldHaveLength, 2)
			So(dataset.Publications[0].HRef, ShouldEqual, validHref)
			So(dataset.Publications[1].HRef, ShouldEqual, validHref)
		})

		Convey("When a Methodologies HRef has whitespace it is trimmed", func() {
			dataset := createDataset()
			dataset.Methodologies[0].HRef = "    " + validHref
			CleanDataset(&dataset)
			So(dataset.Methodologies, ShouldHaveLength, 1)
			So(dataset.Methodologies[0].HRef, ShouldEqual, validHref)
		})

		Convey("When two Methodologies HRef's have whitespace they are trimmed", func() {
			dataset := createDataset()
			dataset.Methodologies[0].HRef = "    " + validHref
			dataset.Methodologies = append(dataset.Methodologies, GeneralDetails{HRef: validHref + "    "})
			CleanDataset(&dataset)
			So(dataset.Methodologies, ShouldHaveLength, 2)
			So(dataset.Methodologies[0].HRef, ShouldEqual, validHref)
			So(dataset.Methodologies[1].HRef, ShouldEqual, validHref)
		})

		Convey("When a RelatedDatasets HRef has whitespace it is trimmed", func() {
			dataset := createDataset()
			dataset.RelatedDatasets[0].HRef = "    " + validHref
			CleanDataset(&dataset)
			So(dataset.RelatedDatasets, ShouldHaveLength, 1)
			So(dataset.RelatedDatasets[0].HRef, ShouldEqual, validHref)
		})

		Convey("When two RelatedDatasets HRef's have whitespace they are trimmed", func() {
			dataset := createDataset()
			dataset.RelatedDatasets[0].HRef = "    " + validHref
			dataset.RelatedDatasets = append(dataset.RelatedDatasets, GeneralDetails{HRef: validHref + "    "})
			CleanDataset(&dataset)
			So(dataset.RelatedDatasets, ShouldHaveLength, 2)
			So(dataset.RelatedDatasets[0].HRef, ShouldEqual, validHref)
			So(dataset.RelatedDatasets[1].HRef, ShouldEqual, validHref)
		})

	})
}

func TestValidateDataset(t *testing.T) {
	t.Parallel()

	Convey("Successful validation (true) returned", t, func() {

		Convey("when dataset.URI contains its path in appropriate url format", func() {
			dataset := createDataset()
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldBeNil)
		})

		Convey("when dataset.URI is empty", func() {
			dataset := createDataset()
			dataset.URI = ""
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldBeNil)
		})

		Convey("when dataset.URI is a relative path", func() {
			dataset := createDataset()
			dataset.URI = "/relative_path"
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldBeNil)
		})

		Convey("when dataset.URI has a valid host but an empty path", func() {
			dataset := createDataset()
			dataset.URI = "http://domain.com/"
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldBeNil)
		})

		Convey("when dataset.URI is only a valid domain", func() {
			dataset := createDataset()
			dataset.URI = "domain.com"
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldBeNil)
		})
	})

	Convey("Unsuccessful validation (false) returned", t, func() {

		Convey("when dataset.URI is unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.URI = ":foo"
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldNotBeNil)
			So(validationErr.Error(), ShouldResemble, errors.New("invalid fields: [URI]").Error())
		})

		Convey("when dataset.URI has an empty host and path", func() {
			dataset := createDataset()
			dataset.URI = "http://"
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldNotBeNil)
			So(validationErr.Error(), ShouldResemble, errors.New("invalid fields: [URI]").Error())
		})

		Convey("when dataset.URI has an empty host but a non empty path", func() {
			dataset := createDataset()
			dataset.URI = "http:///path"
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldNotBeNil)
			So(validationErr.Error(), ShouldResemble, errors.New("invalid fields: [URI]").Error())
		})

		Convey("when dataset.QMI.Href is unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.QMI.HRef = ":foo"
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldNotBeNil)
			So(validationErr.Error(), ShouldResemble, errors.New("invalid fields: [QMI]").Error())
		})

		Convey("when dataset.Publisher.Href is unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.Publisher.HRef = ":foo"
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldNotBeNil)
			So(validationErr.Error(), ShouldResemble, errors.New("invalid fields: [Publisher]").Error())
		})

		Convey("when Publications href is unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.Publications[0].HRef = invalidHref
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldNotBeNil)
			So(validationErr.Error(), ShouldResemble, errors.New("invalid fields: [Publications[0].HRef]").Error())
		})

		Convey("when Methodologies href is unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.Methodologies[0].HRef = invalidHref
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldNotBeNil)
			So(validationErr.Error(), ShouldResemble, errors.New("invalid fields: [Methodologies[0].HRef]").Error())
		})

		Convey("when RelatedDatasets href is unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.RelatedDatasets[0].HRef = invalidHref
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldNotBeNil)
			So(validationErr.Error(), ShouldResemble, errors.New("invalid fields: [RelatedDatasets[0].HRef]").Error())
		})

		Convey("when all href and URI fields are unable to be parsed into url format", func() {
			dataset := createDataset()
			dataset.URI = invalidHref
			dataset.Publications[0].HRef = invalidHref
			dataset.Methodologies[0].HRef = invalidHref
			dataset.RelatedDatasets[0].HRef = invalidHref
			validationErr := ValidateDataset(&dataset)
			So(validationErr, ShouldNotBeNil)
			So(validationErr.Error(), ShouldResemble, errors.New("invalid fields: [URI Publications[0].HRef RelatedDatasets[0].HRef Methodologies[0].HRef]").Error())
		})

	})

}

func TestValidateVersion(t *testing.T) {
	t.Parallel()
	Convey("Successfully return without any errors", t, func() {
		Convey("when the version state is created", func() {

			err := ValidateVersion(&editionConfirmedVersion)
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
		Convey("when the version state is empty", func() {

			err := ValidateVersion(&Version{State: ""})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldResemble, errors.New("missing state from version").Error())
		})

		Convey("when the version state is set to an invalid value", func() {

			err := ValidateVersion(&Version{State: SubmittedState})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldResemble, errors.New("incorrect state, can be one of the following: edition-confirmed, associated or published").Error())
		})

		Convey("when mandatory fields are missing from version document when state is set to created", func() {

			err := ValidateVersion(&Version{State: EditionConfirmedState})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldResemble, errors.New("missing mandatory fields: [release_date]").Error())
		})

		Convey("when the version state is published but has a collection_id", func() {
			version := &Version{
				ReleaseDate:  "2016-04-04",
				State:        PublishedState,
				CollectionID: "cid01",
			}

			err := ValidateVersion(version)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldResemble, errors.New("unexpected collection_id in published version").Error())
		})

		Convey("when version downloads are invalid", func() {
			v := &Version{ReleaseDate: "Today", State: EditionConfirmedState}

			v.Downloads = &DownloadList{XLS: &DownloadObject{HRef: "", Size: "2"}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.XLS.HRef"}), v)

			v.Downloads = &DownloadList{CSV: &DownloadObject{HRef: "", Size: "2"}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.CSV.HRef"}), v)

			v.Downloads = &DownloadList{CSVW: &DownloadObject{HRef: "", Size: "2"}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.CSVW.HRef"}), v)

			v.Downloads = &DownloadList{XLS: &DownloadObject{HRef: "/", Size: ""}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.XLS.Size"}), v)

			v.Downloads = &DownloadList{CSV: &DownloadObject{HRef: "/", Size: ""}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.CSV.Size"}), v)

			v.Downloads = &DownloadList{CSVW: &DownloadObject{HRef: "/", Size: ""}}
			assertVersionDownloadError(fmt.Errorf("missing mandatory fields: %v", []string{"Downloads.CSVW.Size"}), v)

			v.Downloads = &DownloadList{XLS: &DownloadObject{HRef: "/", Size: "bob"}}
			assertVersionDownloadError(fmt.Errorf("invalid fields: %v", []string{"Downloads.XLS.Size not a number"}), v)

			v.Downloads = &DownloadList{CSV: &DownloadObject{HRef: "/", Size: "bob"}}
			assertVersionDownloadError(fmt.Errorf("invalid fields: %v", []string{"Downloads.CSV.Size not a number"}), v)

			v.Downloads = &DownloadList{CSVW: &DownloadObject{HRef: "/", Size: "bob"}}
			assertVersionDownloadError(fmt.Errorf("invalid fields: %v", []string{"Downloads.CSVW.Size not a number"}), v)
		})
	})
}

func assertVersionDownloadError(expected error, v *Version) {
	err := ValidateVersion(v)
	So(err, ShouldNotBeNil)
	So(err, ShouldResemble, expected)
}

func TestCreateDownloadList(t *testing.T) {
	Convey("invalid input bytes return the expected error", t, func() {
		reader := bytes.NewReader([]byte("hello"))
		dl, err := CreateDownloadList(reader)
		So(dl, ShouldBeNil)
		So(reflect.TypeOf(errors.Cause(err)), ShouldEqual, reflect.TypeOf(&json.SyntaxError{}))
	})

	Convey("valid input returns the expected value", t, func() {
		expected := &DownloadList{
			XLS: &DownloadObject{
				Size: "1",
				HRef: "2",
			},
		}

		input, _ := json.Marshal(expected)
		reader := bytes.NewReader(input)

		dl, err := CreateDownloadList(reader)
		So(err, ShouldBeNil)
		So(dl, ShouldResemble, expected)
	})

}

func TestUpdateLinks(t *testing.T) {
	host := "example.com"

	Convey("Given a new edition with no links", t, func() {
		edition := &EditionUpdate{
			ID: "test",
			Next: &Edition{
				ID:      "test",
				Edition: "time-series",
			},
		}

		Convey("when UpdateLinks is called", func() {
			err := edition.UpdateLinks(testContext, host)

			Convey("then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "editions links do not exist")
			})
		})
	})

	Convey("Given an edition with only unpublished versions ", t, func() {
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

		Convey("when UpdateLinks is called", func() {
			err := edition.UpdateLinks(testContext, host)

			Convey("then links are correctly updated", func() {
				So(err, ShouldBeNil)
				So(edition.Next.Links.LatestVersion.ID, ShouldEqual, "2")
				So(edition.Current, ShouldBeNil)
			})
		})
	})

	Convey("Given an edition with a published version ", t, func() {
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

		Convey("when UpdateLinks is called", func() {
			err := edition.UpdateLinks(testContext, host)
			Convey("then links are correctly updated", func() {
				So(err, ShouldBeNil)
				So(edition.Next.Links.LatestVersion.ID, ShouldEqual, "2")
				So(edition.Current.Links.LatestVersion.ID, ShouldEqual, "1")
			})
		})

		Convey("when UpdateLinks is called with a version ID which is lower than the latest published version", func() {
			edition.Current.Links.LatestVersion.ID = "3"
			err := edition.UpdateLinks(testContext, host)
			Convey("then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, "published edition links to a higher version than the requested change")
			})
		})

		Convey("when UpdateLinks is called on an edition with an invalid current version ID", func() {
			edition.Current.Links.LatestVersion.ID = "hi"
			err := edition.UpdateLinks(testContext, host)
			Convey("then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, "failed to convert version id from edition.current document: strconv.Atoi: parsing \"hi\": invalid syntax")
			})
		})

		Convey("when UpdateLinks is called on an edition with an invalid next version ID", func() {
			edition.Next.Links.LatestVersion.ID = "there"
			err := edition.UpdateLinks(testContext, host)
			Convey("then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, "failed to convert version id from edition.next document: strconv.Atoi: parsing \"there\": invalid syntax")
			})
		})
	})
}

func TestPublishLinks(t *testing.T) {
	host := "example.com"

	Convey("Given a new edition with no links", t, func() {
		edition := &EditionUpdate{
			ID: "test",
			Next: &Edition{
				ID:      "test",
				Edition: "time-series",
			},
		}

		Convey("when PublishLinks is called", func() {
			err := edition.PublishLinks(testContext, host, nil)

			Convey("then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "editions links do not exist")
			})
		})
	})

	Convey("Given an edition with an invalid current version ID", t, func() {
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

		Convey("when PublishLinks is called", func() {
			err := edition.PublishLinks(testContext, host, nil)

			Convey("then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, "strconv.Atoi: parsing \"hello\": invalid syntax")
			})
		})
	})

	Convey("Given an edition with only unpublished versions ", t, func() {
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

		Convey("when PublishLinks is called with an invalid version link", func() {
			err := edition.PublishLinks(testContext, host, nil)

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, "invalid arguments to PublishLinks - versionLink empty")
			})
		})

		Convey("when PublishLinks is called with an invalid version link ID", func() {
			err := edition.PublishLinks(testContext, host, &LinkObject{
				ID: "hello",
			})

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, "strconv.Atoi: parsing \"hello\": invalid syntax")
			})
		})

		Convey("when PublishLinks is called with a version link", func() {
			err := edition.PublishLinks(testContext, host, version)

			Convey("then links are correctly updated", func() {
				So(err, ShouldBeNil)
				So(edition.Next.Links.LatestVersion, ShouldEqual, version)
				So(edition.Current, ShouldBeNil)
			})
		})
	})

	Convey("Given an edition with a published version ", t, func() {
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

		Convey("when PublishLinks is called", func() {
			argLink := &LinkObject{
				ID:   "3",
				HRef: "example.com/datasets/1/editions/time-series/versions/3",
			}

			err := edition.PublishLinks(testContext, host, argLink)

			Convey("then links are correctly updated", func() {
				So(err, ShouldBeNil)
				So(edition.Next.Links.LatestVersion, ShouldEqual, argLink)
				So(edition.Current.Links.LatestVersion, ShouldEqual, publishedVersion)
			})
		})

		Convey("when PublishLinks is called with a version ID which is lower than the latest published version", func() {
			argLink := &LinkObject{
				ID:   "1",
				HRef: "example.com/datasets/1/editions/time-series/versions/1",
			}
			err := edition.PublishLinks(testContext, host, argLink)

			Convey("then no changes should be made", func() {
				So(err, ShouldBeNil)
				So(edition.Next.Links.LatestVersion, ShouldEqual, storedNextVersion)
				So(edition.Current.Links.LatestVersion, ShouldEqual, publishedVersion)
			})
		})
	})

}

func TestValidateVersionNumberSuccess(t *testing.T) {

	Convey("Given valid version number above 0 in string format", t, func() {
		versionStr := "5"

		Convey("When ValidateVersionNumber is called", func() {
			versionNumber, err := ValidateVersionNumber(testContext, versionStr)

			Convey("Then no error should be returned", func() {
				So(err, ShouldBeNil)

				Convey("And version number is converted to integer successfully ", func() {
					So(versionNumber, ShouldEqual, 5)
					So(fmt.Sprintf("%T", versionNumber), ShouldEqual, "int")
				})

			})
		})
	})
}

func TestValidateVersionNumberFailure(t *testing.T) {

	Convey("Given invalid version number in string format", t, func() {
		versionStr := "abc"

		Convey("When ValidateVersionNumber is called", func() {
			_, err := ValidateVersionNumber(testContext, versionStr)

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errs.ErrInvalidVersion)
			})
		})
	})

	Convey("Given version number less than 0 in string format", t, func() {
		versionStr := "-1"

		Convey("When ValidateVersionNumber is called", func() {
			_, err := ValidateVersionNumber(testContext, versionStr)

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errs.ErrInvalidVersion)
			})
		})
	})
}
