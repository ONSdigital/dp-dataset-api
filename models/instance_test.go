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
			ImportTasks: &InstanceImportTasks{
				BuildHierarchyTasks: []*BuildHierarchyTask{
					{DimensionID: "dim1"},
				},
				BuildSearchIndexTasks: []*BuildSearchIndexTask{
					{GenericTaskDetails{
						DimensionName: "dim2",
						State:         CreatedState,
					}},
				},
				ImportObservations: &ImportObservationsTask{
					InsertedObservations: 7,
					State:                CreatedState,
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

			Convey("Then if a BuildHierarchyTasks changes, its hash changes", func() {
				instance.ImportTasks.BuildHierarchyTasks[0].State = CompletedState
				hash, err := instance.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldNotEqual, h)
			})

			Convey("Then if a BuildSearchIndexTasks changes, its hash changes", func() {
				instance.ImportTasks.BuildSearchIndexTasks[0].State = CompletedState
				hash, err := instance.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldNotEqual, h)
			})

			Convey("Then if the ImportObservations changes, its hash changes", func() {
				instance.ImportTasks.ImportObservations.State = CompletedState
				hash, err := instance.Hash(nil)
				So(err, ShouldBeNil)
				So(hash, ShouldNotEqual, h)
			})
		})
	})
}
