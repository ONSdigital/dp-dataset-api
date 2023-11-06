package models

import (
	"errors"
	"testing"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
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
			So(err, ShouldEqual, errs.ErrMissingParameters)
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
			So(err, ShouldEqual, errs.ErrMissingParameters)
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
			So(err, ShouldEqual, errs.ErrMissingParameters)
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
			So(err, ShouldEqual, errs.ErrMissingParameters)
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
			So(err.Error(), ShouldEqual, "bad request - invalid instance state: gobbledygook")
		})
	})
}

func TestInstanceHash(t *testing.T) {
	testInstance := func() Instance {
		return Instance{
			InstanceID: "myInstance",
			Edition:    "myEdition",
			State:      CreatedState,
			Version:    1,
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
		}
	}

	Convey("Given an instance with some data", t, func() {
		instance := testInstance()

		Convey("We can generate a valid hash", func() {
			h, err := instance.Hash(nil)
			So(err, ShouldBeNil)
			So(len(h), ShouldEqual, 40)

			Convey("Then hashing it twice, produces the same result", func() {
				hash, err := instance.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldEqual, h)
			})

			Convey("Then storing the hash as its ETag value and hashing it again, produces the same result (field is ignored) and ETag field is preserved", func() {
				instance.ETag = h
				hash, err := instance.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldEqual, h)
				So(instance.ETag, ShouldEqual, h)
			})

			Convey("Then another instance with exactly the same data will resolve to the same hash", func() {
				instance2 := testInstance()
				hash, err := instance2.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldEqual, h)
			})

			Convey("Then if a instance value is modified, its hash changes", func() {
				instance.State = CompletedState
				hash, err := instance.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldNotEqual, h)
			})

			Convey("Then if a dimension is added to the instance, its hash changes", func() {
				instance.Dimensions = append(instance.Dimensions, Dimension{Name: "dim3"})
				hash, err := instance.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldNotEqual, h)
			})

			Convey("Then if a dimension is removed from the instance, its hash changes", func() {
				instance.Dimensions = []Dimension{
					{
						HRef: "http://dimensions.co.uk/dim1",
						Name: "dim1",
					},
				}
				hash, err := instance.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldNotEqual, h)
			})
		})
	})
}
