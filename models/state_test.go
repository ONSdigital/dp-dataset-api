package models

import (
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/smartystreets/goconvey/convey"
)

func TestCheckState(t *testing.T) {
	convey.Convey("Successfully return without any errors", t, func() {
		convey.Convey("when the version has state of edition-confirmed", func() {
			err := CheckState("version", EditionConfirmedState)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when the version has state of associated", func() {
			err := CheckState("version", AssociatedState)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when the version has state of published", func() {
			err := CheckState("version", PublishedState)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when a resource has state of created", func() {
			err := CheckState("resource", CreatedState)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when a resource has state of completed", func() {
			err := CheckState("resource", CompletedState)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when a resource has state of edition-confirmed", func() {
			err := CheckState("resource", EditionConfirmedState)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when a resource has state of associated", func() {
			err := CheckState("resource", AssociatedState)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when a resource has state of published", func() {
			err := CheckState("resource", PublishedState)
			convey.So(err, convey.ShouldBeNil)
		})
	})

	convey.Convey("Return with errors", t, func() {
		convey.Convey("when the version has a missing state", func() {
			err := CheckState("version", "")
			convey.So(err, convey.ShouldEqual, errs.ErrResourceState)
		})

		convey.Convey("when the version has state of gobbly-gook", func() {
			err := CheckState("version", "gobbly-gook")
			convey.So(err, convey.ShouldEqual, errs.ErrResourceState)
		})

		convey.Convey("when a version has state of created", func() {
			err := CheckState("version", CreatedState)
			convey.So(err, convey.ShouldEqual, errs.ErrResourceState)
		})

		convey.Convey("when a version has state of completed", func() {
			err := CheckState("version", CompletedState)
			convey.So(err, convey.ShouldEqual, errs.ErrResourceState)
		})

		convey.Convey("when the resource has a missing state", func() {
			err := CheckState("resource", "")
			convey.So(err, convey.ShouldEqual, errs.ErrResourceState)
		})

		convey.Convey("when the resource has state of gobbly-gook", func() {
			err := CheckState("resource", "gobbly-gook")
			convey.So(err, convey.ShouldEqual, errs.ErrResourceState)
		})
	})
}
