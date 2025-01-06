package models

import (
	"errors"
	"testing"
	"time"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/smartystreets/goconvey/convey"
)

func TestValidateStateFilter(t *testing.T) {
	t.Parallel()
	convey.Convey("Successfully return without any errors", t, func() {
		convey.Convey("when the filter list contains a state of `created`", func() {
			err := ValidateStateFilter([]string{CreatedState})
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when the filter list contains a state of `submitted`", func() {
			err := ValidateStateFilter([]string{SubmittedState})
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when the filter list contains a state of `completed`", func() {
			err := ValidateStateFilter([]string{CompletedState})
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when the filter list contains a state of `edition-confirmed`", func() {
			err := ValidateStateFilter([]string{EditionConfirmedState})
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when the filter list contains a state of `associated`", func() {
			err := ValidateStateFilter([]string{AssociatedState})
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when the filter list contains a state of `published`", func() {
			err := ValidateStateFilter([]string{PublishedState})
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("when the filter list contains more than one valid state", func() {
			err := ValidateStateFilter([]string{EditionConfirmedState, PublishedState})
			convey.So(err, convey.ShouldBeNil)
		})
	})

	convey.Convey("Return with errors", t, func() {
		convey.Convey("when the filter list contains an invalid state", func() {
			err := ValidateStateFilter([]string{"foo"})
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldResemble, errors.New("bad request - invalid filter state values: [foo]"))
		})

		convey.Convey("when the filter list contains more than one invalid state", func() {
			err := ValidateStateFilter([]string{"foo", "bar"})
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldResemble, errors.New("bad request - invalid filter state values: [foo bar]"))
		})
	})
}

func TestValidateEvent(t *testing.T) {
	currentTime := time.Now().UTC()

	t.Parallel()
	convey.Convey("Given an event contains all mandatory fields", t, func() {
		convey.Convey("Then successfully return without any errors ", func() {
			event := &Event{
				Message:       "test message",
				MessageOffset: "56",
				Time:          &currentTime,
				Type:          "error",
			}
			err := event.Validate()
			convey.So(err, convey.ShouldBeNil)
		})
	})

	convey.Convey("Given event is missing 'message' field from event", t, func() {
		convey.Convey("Then event fails validation and returns an error 'missing properties'", func() {
			event := &Event{
				MessageOffset: "56",
				Time:          &currentTime,
				Type:          "error",
			}
			err := event.Validate()
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldEqual, errs.ErrMissingParameters)
		})
	})

	convey.Convey("Given event is missing 'message_offset' field from event", t, func() {
		convey.Convey("Then event fails validation and returns an error 'missing properties'", func() {
			event := &Event{
				Message: "test message",
				Time:    &currentTime,
				Type:    "error",
			}
			err := event.Validate()
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldEqual, errs.ErrMissingParameters)
		})
	})

	convey.Convey("Given event is missing 'time' field from event", t, func() {
		convey.Convey("Then event fails validation and returns an error 'missing properties'", func() {
			event := &Event{
				Message:       "test message",
				MessageOffset: "56",
				Type:          "error",
			}
			err := event.Validate()
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldEqual, errs.ErrMissingParameters)
		})
	})

	convey.Convey("Given event is missing 'type' field from event", t, func() {
		convey.Convey("Then event fails validation and returns an error 'missing properties'", func() {
			event := &Event{
				Message:       "test message",
				MessageOffset: "56",
				Time:          &currentTime,
			}
			err := event.Validate()
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldEqual, errs.ErrMissingParameters)
		})
	})
}

func TestValidateInstanceState(t *testing.T) {
	t.Parallel()
	convey.Convey("Given a state of 'created'", t, func() {
		convey.Convey("Then successfully return without any errors", func() {
			err := ValidateInstanceState(CreatedState)
			convey.So(err, convey.ShouldBeNil)
		})
	})

	convey.Convey("Given a state of 'submitted'", t, func() {
		convey.Convey("Then successfully return without any errors ", func() {
			err := ValidateInstanceState(SubmittedState)
			convey.So(err, convey.ShouldBeNil)
		})
	})

	convey.Convey("Given a state of 'completed'", t, func() {
		convey.Convey("Then successfully return without any errors", func() {
			err := ValidateInstanceState(CompletedState)
			convey.So(err, convey.ShouldBeNil)
		})
	})

	convey.Convey("Given a state of 'edition-confirmed'", t, func() {
		convey.Convey("Then successfully return without any errors", func() {
			err := ValidateInstanceState(EditionConfirmedState)
			convey.So(err, convey.ShouldBeNil)
		})
	})

	convey.Convey("Given a state of 'associated'", t, func() {
		convey.Convey("Then successfully return without any errors", func() {
			err := ValidateInstanceState(AssociatedState)
			convey.So(err, convey.ShouldBeNil)
		})
	})

	convey.Convey("Given a state of 'published'", t, func() {
		convey.Convey("Then successfully return without any errors", func() {
			err := ValidateInstanceState(PublishedState)
			convey.So(err, convey.ShouldBeNil)
		})
	})

	convey.Convey("Given a state of 'gobbledygook'", t, func() {
		convey.Convey("Then validation of state fails and returns an error", func() {
			err := ValidateInstanceState("gobbledygook")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "bad request - invalid instance state: gobbledygook")
		})
	})
}

func TestValidateImportTask(t *testing.T) {
	t.Parallel()
	convey.Convey("Given an import task contains all mandatory fields and state is set to 'completed'", t, func() {
		convey.Convey("Then successfully return without any errors", func() {
			task := GenericTaskDetails{
				DimensionName: "geography",
				State:         CompletedState,
			}
			err := ValidateImportTask(task)
			convey.So(err, convey.ShouldBeNil)
		})
	})

	convey.Convey("Given an import task is missing mandatory field 'dimension_name'", t, func() {
		convey.Convey("Then import task fails validation and returns an error", func() {
			task := GenericTaskDetails{
				State: CompletedState,
			}
			err := ValidateImportTask(task)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "bad request - missing mandatory fields: [dimension_name]")
		})
	})

	convey.Convey("Given an import task is missing mandatory field 'state'", t, func() {
		convey.Convey("Then import task fails validation and returns error", func() {
			task := GenericTaskDetails{
				DimensionName: "geography",
			}
			err := ValidateImportTask(task)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "bad request - missing mandatory fields: [state]")
		})
	})

	convey.Convey("Given an import task is missing mandatory field 'state' and 'dimension_name'", t, func() {
		convey.Convey("Then import task fails validation and returns an error", func() {
			task := GenericTaskDetails{}
			err := ValidateImportTask(task)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "bad request - missing mandatory fields: [dimension_name state]")
		})
	})

	convey.Convey("Given an import task contains an invalid state, 'submitted'", t, func() {
		convey.Convey("Then import task fails validation and returns an error", func() {
			task := GenericTaskDetails{
				DimensionName: "geography",
				State:         SubmittedState,
			}
			err := ValidateImportTask(task)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "bad request - invalid task state value: submitted")
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

	convey.Convey("Given an instance with some data", t, func() {
		instance := testInstance()

		convey.Convey("We can generate a valid hash", func() {
			h, err := instance.Hash(nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(len(h), convey.ShouldEqual, 40)

			convey.Convey("Then hashing it twice, produces the same result", func() {
				hash, err := instance.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldEqual, h)
			})

			convey.Convey("Then storing the hash as its ETag value and hashing it again, produces the same result (field is ignored) and ETag field is preserved", func() {
				instance.ETag = h
				hash, err := instance.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldEqual, h)
				convey.So(instance.ETag, convey.ShouldEqual, h)
			})

			convey.Convey("Then another instance with exactly the same data will resolve to the same hash", func() {
				instance2 := testInstance()
				hash, err := instance2.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldEqual, h)
			})

			convey.Convey("Then if a instance value is modified, its hash changes", func() {
				instance.State = CompletedState
				hash, err := instance.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldNotEqual, h)
			})

			convey.Convey("Then if a dimension is added to the instance, its hash changes", func() {
				instance.Dimensions = append(instance.Dimensions, Dimension{Name: "dim3"})
				hash, err := instance.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldNotEqual, h)
			})

			convey.Convey("Then if a dimension is removed from the instance, its hash changes", func() {
				instance.Dimensions = []Dimension{
					{
						HRef: "http://dimensions.co.uk/dim1",
						Name: "dim1",
					},
				}
				hash, err := instance.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldNotEqual, h)
			})

			convey.Convey("Then if a BuildHierarchyTasks changes, its hash changes", func() {
				instance.ImportTasks.BuildHierarchyTasks[0].State = CompletedState
				hash, err := instance.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldNotEqual, h)
			})

			convey.Convey("Then if a BuildSearchIndexTasks changes, its hash changes", func() {
				instance.ImportTasks.BuildSearchIndexTasks[0].State = CompletedState
				hash, err := instance.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldNotEqual, h)
			})

			convey.Convey("Then if the ImportObservations changes, its hash changes", func() {
				instance.ImportTasks.ImportObservations.State = CompletedState
				hash, err := instance.Hash(nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(hash, convey.ShouldNotEqual, h)
			})
		})
	})
}
