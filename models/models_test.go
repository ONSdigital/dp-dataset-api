package models

import (
	"errors"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

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
