package instance_test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_ConfirmEditionReturnsOK(t *testing.T) {
	Convey("given no edition exists", t, func() {
		Convey("when confirmEdition is called", func() {
			Convey("then an edition is created and the version ID is 1", func() {

			})
		})
	})

	Convey("given an edition exists with 1 version", t, func() {
		Convey("when confirmEdition is called", func() {
			Convey("then the edition is updated and the version ID is 2", func() {

			})
		})
	})

	Convey("given an edition exists with 10 version", t, func() {
		Convey("when confirmEdition is called", func() {
			Convey("then the edition is updated and the version ID is 11", func() {

			})
		})
	})
}

func Test_ConfirmEditionReturnsError(t *testing.T) {
	Convey("given the datastore is unavailable", t, func() {
		Convey("when confirmEdition is called", func() {
			Convey("then an error is returned", func() {

			})
		})
	})

	Convey("given an invalid edition exists", t, func() {
		Convey("when confirmEdition is called", func() {
			Convey("then updating links fails and an error is returned", func() {

			})
		})
	})

	Convey("given intermittent datastore failures", t, func() {
		Convey("when confirmEdition is called and updating the datastore for the edition fails", func() {
			Convey("then an error is returned", func() {

			})
		})
	})
}
