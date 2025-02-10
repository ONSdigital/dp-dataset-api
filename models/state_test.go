package models

import (
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCheckState(t *testing.T) {
	Convey("Successfully return without any errors", t, func() {
		Convey("when the version has state of edition-confirmed", func() {
			err := CheckState("version", EditionConfirmedState)
			So(err, ShouldBeNil)
		})

		Convey("when the version has state of associated", func() {
			err := CheckState("version", AssociatedState)
			So(err, ShouldBeNil)
		})

		Convey("when the version has state of published", func() {
			err := CheckState("version", PublishedState)
			So(err, ShouldBeNil)
		})

		Convey("when a resource has state of created", func() {
			err := CheckState("resource", CreatedState)
			So(err, ShouldBeNil)
		})

		Convey("when a resource has state of completed", func() {
			err := CheckState("resource", CompletedState)
			So(err, ShouldBeNil)
		})

		Convey("when a resource has state of edition-confirmed", func() {
			err := CheckState("resource", EditionConfirmedState)
			So(err, ShouldBeNil)
		})

		Convey("when a resource has state of associated", func() {
			err := CheckState("resource", AssociatedState)
			So(err, ShouldBeNil)
		})

		Convey("when a resource has state of published", func() {
			err := CheckState("resource", PublishedState)
			So(err, ShouldBeNil)
		})
	})

	Convey("Return with errors", t, func() {
		Convey("when the version has a missing state", func() {
			err := CheckState("version", "")
			So(err, ShouldEqual, errs.ErrResourceState)
		})

		Convey("when the version has state of gobbly-gook", func() {
			err := CheckState("version", "gobbly-gook")
			So(err, ShouldEqual, errs.ErrResourceState)
		})

		Convey("when a version has state of created", func() {
			err := CheckState("version", CreatedState)
			So(err, ShouldEqual, errs.ErrResourceState)
		})

		Convey("when a version has state of completed", func() {
			err := CheckState("version", CompletedState)
			So(err, ShouldEqual, errs.ErrResourceState)
		})

		Convey("when the resource has a missing state", func() {
			err := CheckState("resource", "")
			So(err, ShouldEqual, errs.ErrResourceState)
		})

		Convey("when the resource has state of gobbly-gook", func() {
			err := CheckState("resource", "gobbly-gook")
			So(err, ShouldEqual, errs.ErrResourceState)
		})
	})
}
