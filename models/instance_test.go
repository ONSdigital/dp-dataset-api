package models

import (
	"errors"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestValidateStateFilter(t *testing.T) {
	t.Parallel()
	Convey("Successfully return without any errors", t, func() {
		Convey("when the filter list contains a state of `created`", func() {

			err := ValidateStateFilter([]string{"created"})
			So(err, ShouldBeNil)
		})

		Convey("when the filter list contains a state of `submitted`", func() {

			err := ValidateStateFilter([]string{"submitted"})
			So(err, ShouldBeNil)
		})

		Convey("when the filter list contains a state of `completed`", func() {

			err := ValidateStateFilter([]string{"completed"})
			So(err, ShouldBeNil)
		})

		Convey("when the filter list contains a state of `edition-confirmed`", func() {

			err := ValidateStateFilter([]string{"edition-confirmed"})
			So(err, ShouldBeNil)
		})

		Convey("when the filter list contains a state of `associated`", func() {

			err := ValidateStateFilter([]string{"associated"})
			So(err, ShouldBeNil)
		})

		Convey("when the filter list contains a state of `published`", func() {

			err := ValidateStateFilter([]string{"published"})
			So(err, ShouldBeNil)
		})

		Convey("when the filter list contains more than one valid state", func() {

			err := ValidateStateFilter([]string{"edition-confirmed", "published"})
			So(err, ShouldBeNil)
		})
	})

	Convey("Return with errors", t, func() {
		Convey("when the filter list contains an invalid state", func() {

			err := ValidateStateFilter([]string{"foo"})
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errors.New("invalid filter state values"))
		})

		Convey("when the filter list contains more than one invalid state", func() {

			err := ValidateStateFilter([]string{"foo", "bar"})
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errors.New("invalid filter state values"))
		})
	})
}
