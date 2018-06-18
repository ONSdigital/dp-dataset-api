package models

import (
	"errors"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestValidateStateFilter(t *testing.T) {
	t.Parallel()
	Convey("Successfully return without any errors", t, func() {
		Convey("when the filter list contains a state of `created`", func() {

			err := ValidateStateFilter([]string{CreatedState})
			So(err, ShouldBeNil)
		})

		Convey("when the filter list contains a state of `submitted`", func() {

			err := ValidateStateFilter([]string{SubmittedState})
			So(err, ShouldBeNil)
		})

		Convey("when the filter list contains a state of `completed`", func() {

			err := ValidateStateFilter([]string{CompletedState})
			So(err, ShouldBeNil)
		})

		Convey("when the filter list contains a state of `edition-confirmed`", func() {

			err := ValidateStateFilter([]string{EditionConfirmedState})
			So(err, ShouldBeNil)
		})

		Convey("when the filter list contains a state of `associated`", func() {

			err := ValidateStateFilter([]string{AssociatedState})
			So(err, ShouldBeNil)
		})

		Convey("when the filter list contains a state of `published`", func() {

			err := ValidateStateFilter([]string{PublishedState})
			So(err, ShouldBeNil)
		})

		Convey("when the filter list contains more than one valid state", func() {

			err := ValidateStateFilter([]string{EditionConfirmedState, PublishedState})
			So(err, ShouldBeNil)
		})
	})

	Convey("Return with errors", t, func() {
		Convey("when the filter list contains an invalid state", func() {

			err := ValidateStateFilter([]string{"foo"})
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errors.New("bad request - invalid filter state values: [foo]"))
		})

		Convey("when the filter list contains more than one invalid state", func() {

			err := ValidateStateFilter([]string{"foo", "bar"})
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errors.New("bad request - invalid filter state values: [foo bar]"))
		})
	})
}

func TestValidateEvent(t *testing.T) {
	currentTime := time.Now().UTC()

	t.Parallel()
	Convey("Given an event contains all mandatory fields", t, func() {
		Convey("Then successfully return without any errors ", func() {
			event := &Event{
				Message:       "test message",
				MessageOffset: "56",
				Time:          &currentTime,
				Type:          "error",
			}
			err := event.Validate()
			So(err, ShouldBeNil)
		})
	})

	Convey("Given event is missing 'message' field from event", t, func() {
		Convey("Then event fails validation and returns an error 'missing properties'", func() {
			event := &Event{
				MessageOffset: "56",
				Time:          &currentTime,
				Type:          "error",
			}
			err := event.Validate()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "missing properties")
		})
	})

	Convey("Given event is missing 'message_offset' field from event", t, func() {
		Convey("Then event fails validation and returns an error 'missing properties'", func() {
			event := &Event{
				Message: "test message",
				Time:    &currentTime,
				Type:    "error",
			}
			err := event.Validate()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "missing properties")
		})
	})

	Convey("Given event is missing 'time' field from event", t, func() {
		Convey("Then event fails validation and returns an error 'missing properties'", func() {
			event := &Event{
				Message:       "test message",
				MessageOffset: "56",
				Type:          "error",
			}
			err := event.Validate()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "missing properties")
		})
	})

	Convey("Given event is missing 'type' field from event", t, func() {
		Convey("Then event fails validation and returns an error 'missing properties'", func() {
			event := &Event{
				Message:       "test message",
				MessageOffset: "56",
				Time:          &currentTime,
			}
			err := event.Validate()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "missing properties")
		})
	})
}

func TestValidateInstanceState(t *testing.T) {

	t.Parallel()
	Convey("Given a state of 'created'", t, func() {
		Convey("Then successfully return without any errors", func() {
			err := ValidateInstanceState(CreatedState)
			So(err, ShouldBeNil)
		})
	})

	Convey("Given a state of 'submitted'", t, func() {
		Convey("Then successfully return without any errors ", func() {
			err := ValidateInstanceState(SubmittedState)
			So(err, ShouldBeNil)
		})
	})

	Convey("Given a state of 'completed'", t, func() {
		Convey("Then successfully return without any errors", func() {
			err := ValidateInstanceState(CompletedState)
			So(err, ShouldBeNil)
		})
	})

	Convey("Given a state of 'edition-confirmed'", t, func() {
		Convey("Then successfully return without any errors", func() {
			err := ValidateInstanceState(EditionConfirmedState)
			So(err, ShouldBeNil)
		})
	})

	Convey("Given a state of 'associated'", t, func() {
		Convey("Then successfully return without any errors", func() {
			err := ValidateInstanceState(AssociatedState)
			So(err, ShouldBeNil)
		})
	})

	Convey("Given a state of 'published'", t, func() {
		Convey("Then successfully return without any errors", func() {
			err := ValidateInstanceState(PublishedState)
			So(err, ShouldBeNil)
		})
	})

	Convey("Given a state of 'gobbledygook'", t, func() {
		Convey("Then validation of state fails and returns an error", func() {
			err := ValidateInstanceState("gobbledygook")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "bad request - invalid filter state values: [gobbledygook]")
		})
	})
}

func TestValidateImportTask(t *testing.T) {

	t.Parallel()
	Convey("Given an import task contains all mandatory fields and state is set to 'completed'", t, func() {
		Convey("Then successfully return without any errors", func() {
			task := GenericTaskDetails{
				DimensionName: "geography",
				State:         CompletedState,
			}
			err := ValidateImportTask(task)
			So(err, ShouldBeNil)
		})
	})

	Convey("Given an import task is missing mandatory field 'dimension_name'", t, func() {
		Convey("Then import task fails validation and returns an error", func() {
			task := GenericTaskDetails{
				State: CompletedState,
			}
			err := ValidateImportTask(task)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "bad request - missing mandatory fields: [dimension_name]")
		})
	})

	Convey("Given an import task is missing mandatory field 'state'", t, func() {
		Convey("Then import task fails validation and returns error", func() {
			task := GenericTaskDetails{
				DimensionName: "geography",
			}
			err := ValidateImportTask(task)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "bad request - missing mandatory fields: [state]")
		})
	})

	Convey("Given an import task is missing mandatory field 'state' and 'dimension_name'", t, func() {
		Convey("Then import task fails validation and returns an error", func() {
			task := GenericTaskDetails{}
			err := ValidateImportTask(task)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "bad request - missing mandatory fields: [dimension_name state]")
		})
	})

	Convey("Given an import task contains an invalid state, 'submitted'", t, func() {
		Convey("Then import task fails validation and returns an error", func() {
			task := GenericTaskDetails{
				DimensionName: "geography",
				State:         SubmittedState,
			}
			err := ValidateImportTask(task)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "bad request - invalid task state value: submitted")
		})
	})
}
