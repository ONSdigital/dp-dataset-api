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
			result := CantabularBlob.String()
			So(result, ShouldEqual, "cantabular_blob")
			So(datasetTypes[0], ShouldEqual, "cantabular_table")
			So(datasetTypes[1], ShouldEqual, "cantabular_blob")
			So(datasetTypes[2], ShouldEqual, "cantabular_flexible_table")
			So(datasetTypes[3], ShouldEqual, "cantabular_multivariate_table")
			So(datasetTypes[4], ShouldEqual, "invalid")
		})
	})
}

func TestGetDatasetType(t *testing.T) {
	Convey("Given the dataset type", t, func() {
		//TODO: Replace this test with 'static' default and adjust the TestString above
		// Convey("When the type is empty", func() {
		// 	Convey("Then it should default to filterable", func() {
		// 		result, err := GetDatasetType("")
		// 		So(result, ShouldEqual, Filterable)
		// 		So(err, ShouldBeNil)
		// 	})
		// })

		Convey("When the type is cantabular_blob", func() {
			Convey("Then it should return the appropriate value", func() {
				result, err := GetDatasetType("cantabular_blob")
				So(result, ShouldEqual, CantabularBlob)
				So(err, ShouldBeNil)
			})
		})

		Convey("When the type is cantabular_table", func() {
			Convey("Then it should return the appropriate value", func() {
				result, err := GetDatasetType("cantabular_table")
				So(result, ShouldEqual, CantabularTable)
				So(err, ShouldBeNil)
			})
		})

		Convey("When the type is cantabular_flexible_table", func() {
			Convey("Then it should return the appropriate value", func() {
				result, err := GetDatasetType("cantabular_flexible_table")
				So(result, ShouldEqual, CantabularFlexibleTable)
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
			So(dataset.Type, ShouldEqual, "cantabular_table")
			So(dataset.CanonicalTopic, ShouldResemble, canonicalTopic)
			So(dataset.Subtopics[0], ShouldResemble, subtopic)
			So(dataset.Survey, ShouldEqual, survey)
			So(dataset.RelatedContent, ShouldResemble, relatedContent)
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

	Convey("Given a version with some data", t, func() {
		v := testVersion()

		Convey("We can generate a valid hash", func() {
			h, err := v.Hash(nil)
			So(err, ShouldBeNil)
			So(len(h), ShouldEqual, 40)

			Convey("Then hashing it twice, produces the same result", func() {
				hash, err := v.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldEqual, h)
			})

			Convey("Then storing the hash as its ETag value and hashing it again, produces the same result (field is ignored) and ETag field is preserved", func() {
				v.ETag = h
				hash, err := v.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldEqual, h)
				So(v.ETag, ShouldEqual, h)
			})

			Convey("Then another version with exactly the same data will resolve to the same hash", func() {
				v2 := testVersion()
				hash, err := v2.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldEqual, h)
			})

			Convey("Then if a version value is modified, its hash changes", func() {
				v.State = CompletedState
				hash, err := v.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldNotEqual, h)
			})

			Convey("Then if a download link is added to the version, its hash changes", func() {
				v.Downloads.TXT = &DownloadObject{
					Private: "private/link.txt",
					HRef:    "downloadservice/link.txt",
				}
				hash, err := v.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldNotEqual, h)
			})

			Convey("Then if a dimension is removed from the version, its hash changes", func() {
				v.Dimensions = []Dimension{
					{
						HRef: "http://dimensions.co.uk/dim1",
						Name: "dim1",
					},
				}
				hash, err := v.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldNotEqual, h)
			})
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
	Convey("Given a new edition with no links", t, func() {
		edition := &EditionUpdate{
			ID: "test",
			Next: &Edition{
				ID:      "test",
				Edition: "time-series",
			},
		}

		Convey("when PublishLinks is called", func() {
			err := edition.PublishLinks(testContext, nil)

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
			err := edition.PublishLinks(testContext, nil)

			Convey("then an error should be returned", func() {
				So(err, ShouldNotBeNil)

				var expError *strconv.NumError
				So(errors.As(err, &expError), ShouldBeTrue)
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
			err := edition.PublishLinks(testContext, nil)

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, "invalid arguments to PublishLinks - versionLink empty")
			})
		})

		Convey("when PublishLinks is called with an invalid version link ID", func() {
			err := edition.PublishLinks(testContext, &LinkObject{
				ID: "hello",
			})

			Convey("then an error is returned", func() {
				So(err, ShouldNotBeNil)
				var expError *strconv.NumError
				So(errors.As(err, &expError), ShouldBeTrue)
			})
		})

		Convey("when PublishLinks is called with a version link", func() {
			err := edition.PublishLinks(testContext, version)

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

			err := edition.PublishLinks(testContext, argLink)

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
			err := edition.PublishLinks(testContext, argLink)

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

		Convey("When ParseAndValidateVersionNumber is called", func() {
			versionNumber, err := ParseAndValidateVersionNumber(testContext, versionStr)

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

		Convey("When ParseAndValidateVersionNumber is called", func() {
			_, err := ParseAndValidateVersionNumber(testContext, versionStr)

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errs.ErrInvalidVersion)
			})
		})
	})

	Convey("Given version number less than 0 in string format", t, func() {
		versionStr := "-1"

		Convey("When ParseAndValidateVersionNumber is called", func() {
			_, err := ParseAndValidateVersionNumber(testContext, versionStr)

			Convey("Then an error should be returned", func() {
				So(err, ShouldNotBeNil)
				So(err, ShouldResemble, errs.ErrInvalidVersion)
			})
		})
	})
}

func TestVersionLinksDeepCopy(t *testing.T) {
	Convey("Given a fully populated VersionLinks", t, func() {
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

		Convey("Then doing a deep copy of it results in a new fully populated VersionLinks", func() {
			vl2 := vl.DeepCopy()
			So(*vl2, ShouldResemble, VersionLinks{
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
			So(vl2, ShouldNotEqual, vl)
			So(vl2.Dataset, ShouldNotEqual, vl.Dataset)
			So(vl2.Dimensions, ShouldNotEqual, vl.Dimensions)
			So(vl2.Edition, ShouldNotEqual, vl.Edition)
			So(vl2.Self, ShouldNotEqual, vl.Self)
			So(vl2.Spatial, ShouldNotEqual, vl.Spatial)
			So(vl2.Version, ShouldNotEqual, vl.Version)
		})
	})

	Convey("Given an empty VersionLinks", t, func() {
		vl := &VersionLinks{}

		Convey("Then doing a deep copy of it results in a new empty VersionLinks", func() {
			vl2 := vl.DeepCopy()
			So(*vl2, ShouldResemble, VersionLinks{})
			So(vl2, ShouldNotEqual, vl)
		})
	})
}

func TestVersionDownloadsOrder(t *testing.T) {
	Convey("Given a Downloads struct", t, func() {
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

		Convey("When marshalled", func() {
			b, err := json.Marshal(d)
			So(err, ShouldBeNil)
			t.Log(string(b))

			Convey("The downloads should be in the expected order", func() {
				expected := `{"xls":{"href":"XLS"},"xlsx":{"href":"XLSX"},"csv":{"href":"CSV"},"txt":{"href":"TXT"},"csvw":{"href":"CSVW"}}`
				So(string(b), ShouldResemble, expected)
			})
		})
	})
}
