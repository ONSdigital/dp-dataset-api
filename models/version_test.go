package models

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

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
			So(version.QualityDesignation, ShouldEqual, QualityDesignationOfficial)
			So(version.Distributions, ShouldResemble, &[]Distribution{distribution})
		})

		Convey("when the version state is associated for a static dataset without collection_id", func() {
			staticVersion := Version{
				ReleaseDate: "2017-10-12",
				State:       AssociatedState,
				Type:        Static.String(),
				Downloads: &DownloadList{
					CSV: &DownloadObject{
						HRef: "test-href",
						Size: "1234",
					},
				},
			}

			err := ValidateVersion(&staticVersion)
			So(err, ShouldBeNil)
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

		Convey("when the version state is approved and type is static", func() {
			err := ValidateVersion(&approvedVersion)
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
			So(err.Error(), ShouldResemble, ErrVersionStateInvalid.Error())
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

		Convey("when the version state is associated for a non-static dataset without collection_id", func() {
			nonStaticVersion := Version{
				ReleaseDate: "2017-10-12",
				State:       AssociatedState,
				Type:        "filterable",
				Downloads: &DownloadList{
					CSV: &DownloadObject{
						HRef: "test-href",
						Size: "1234",
					},
				},
			}

			err := ValidateVersion(&nonStaticVersion)
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, ErrAssociatedVersionCollectionIDInvalid)
		})

		Convey("when the version state is approved for a non-static dataset", func() {
			nonStaticVersion := Version{
				State: ApprovedState,
				Type:  "filterable",
			}

			err := ValidateVersion(&nonStaticVersion)
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, ErrVersionStateDatasetTypeInvalid)
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
			QualityDesignation: QualityDesignationOfficial,
			Distributions:      &[]Distribution{distribution},
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

			So(vl2, ShouldNotPointTo, vl)
			So(vl2.Dataset, ShouldNotPointTo, vl.Dataset)
			So(vl2.Dimensions, ShouldNotPointTo, vl.Dimensions)
			So(vl2.Edition, ShouldNotPointTo, vl.Edition)
			So(vl2.Self, ShouldNotPointTo, vl.Self)
			So(vl2.Spatial, ShouldNotPointTo, vl.Spatial)
			So(vl2.Version, ShouldNotPointTo, vl.Version)
		})
	})

	Convey("Given an empty VersionLinks", t, func() {
		vl := &VersionLinks{}

		Convey("Then doing a deep copy of it results in a new empty VersionLinks", func() {
			vl2 := vl.DeepCopy()
			So(*vl2, ShouldResemble, VersionLinks{})
			So(vl2, ShouldNotPointTo, vl)
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

func TestQualityDesignation_IsValid(t *testing.T) {
	Convey("Given a QualityDesignation", t, func() {
		testCases := []struct {
			qualityDesignation QualityDesignation
			expectedIsValid    bool
		}{
			{QualityDesignationAccreditedOfficial, true},
			{QualityDesignationOfficialInDevelopment, true},
			{QualityDesignationOfficial, true},
			{QualityDesignationNoAccreditation, true},
			{"invalid-designation", false},
			{"", false},
		}

		for _, tc := range testCases {
			Convey("When QualityDesignation is "+tc.qualityDesignation.String(), func() {
				isValid := tc.qualityDesignation.IsValid()

				Convey("Then IsValid should return "+strconv.FormatBool(tc.expectedIsValid), func() {
					So(isValid, ShouldEqual, tc.expectedIsValid)
				})
			})
		}
	})
}
