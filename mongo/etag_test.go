package mongo

import (
	"testing"

	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
)

func testInstance() *models.Instance {
	i := &models.Instance{
		CollectionID: "testCollection",
		Dimensions:   []models.Dimension{{Name: "dim1"}, {Name: "dim2"}},
		Edition:      "testEdition",
		InstanceID:   "123",
		State:        models.CreatedState,
	}
	eTag0, err := i.Hash(nil)
	So(err, ShouldBeNil)
	i.ETag = eTag0
	return i
}

func TestNewETagForUpdate(t *testing.T) {

	Convey("Given an instance", t, func() {

		currentInstance := testInstance()

		update := &models.Instance{
			State: models.CompletedState,
		}

		Convey("getNewETagForUpdate returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForUpdate(currentInstance, update)
			So(err, ShouldBeNil)
			So(eTag1, ShouldNotEqual, currentInstance.ETag)

			Convey("Applying the same update to a different filter results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = "otherInstance"
				eTag2, err := newETagForUpdate(instance2, update)
				So(err, ShouldBeNil)
				So(eTag2, ShouldNotEqual, eTag1)
			})

			Convey("Applying a different update to the same filter results in a different ETag", func() {
				update2 := &models.Instance{
					InstanceID: "anotherInstanceID",
				}
				eTag3, err := newETagForUpdate(currentInstance, update2)
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForAddEvent(t *testing.T) {

	Convey("Given an instance", t, func() {

		currentInstance := testInstance()

		event := models.Event{
			Message: "testEvent",
		}

		Convey("newETagForAddEvent returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForAddEvent(currentInstance, &event)
			So(err, ShouldBeNil)
			So(eTag1, ShouldNotEqual, currentInstance.ETag)

			Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = "otherInstance"
				eTag2, err := newETagForAddEvent(instance2, &event)
				So(err, ShouldBeNil)
				So(eTag2, ShouldNotEqual, eTag1)
			})

			Convey("Applying a different update to the same filter results in a different ETag", func() {
				event = models.Event{
					Message: "anotherEvent",
				}
				eTag3, err := newETagForAddEvent(currentInstance, &event)
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForObservationsInserted(t *testing.T) {

	Convey("Given an instance", t, func() {

		currentInstance := testInstance()

		var obsInserted int64 = 12345

		Convey("newETagForObservationsInserted returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForObservationsInserted(currentInstance, obsInserted)
			So(err, ShouldBeNil)
			So(eTag1, ShouldNotEqual, currentInstance.ETag)

			Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = "otherInstance"
				eTag2, err := newETagForObservationsInserted(instance2, obsInserted)
				So(err, ShouldBeNil)
				So(eTag2, ShouldNotEqual, eTag1)
			})

			Convey("Applying a different update to the same filter results in a different ETag", func() {
				obsInserted = 54321
				eTag3, err := newETagForObservationsInserted(currentInstance, obsInserted)
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForStateUpdate(t *testing.T) {

	Convey("Given an instance", t, func() {

		currentInstance := testInstance()

		Convey("newETagForStateUpdate returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForStateUpdate(currentInstance, models.CompletedState)
			So(err, ShouldBeNil)
			So(eTag1, ShouldNotEqual, currentInstance.ETag)

			Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = "otherInstance"
				eTag2, err := newETagForStateUpdate(instance2, models.CompletedState)
				So(err, ShouldBeNil)
				So(eTag2, ShouldNotEqual, eTag1)
			})

			Convey("Applying a different update to the same filter results in a different ETag", func() {
				eTag3, err := newETagForStateUpdate(currentInstance, models.DetachedState)
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForHierarchyTaskStateUpdate(t *testing.T) {

	Convey("Given an instance", t, func() {

		currentInstance := testInstance()
		dimension := "dim1"

		Convey("newETagForHierarchyTaskStateUpdate returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForHierarchyTaskStateUpdate(currentInstance, dimension, models.CompletedState)
			So(err, ShouldBeNil)
			So(eTag1, ShouldNotEqual, currentInstance.ETag)

			Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = "otherInstance"
				eTag2, err := newETagForHierarchyTaskStateUpdate(instance2, dimension, models.CompletedState)
				So(err, ShouldBeNil)
				So(eTag2, ShouldNotEqual, eTag1)
			})

			Convey("Applying a different update to the same filter results in a different ETag", func() {
				eTag3, err := newETagForHierarchyTaskStateUpdate(currentInstance, dimension, models.DetachedState)
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForBuildSearchTaskStateUpdate(t *testing.T) {

	Convey("Given an instance", t, func() {

		currentInstance := testInstance()
		dimension := "dim1"

		Convey("newETagForBuildSearchTaskStateUpdate returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForBuildSearchTaskStateUpdate(currentInstance, dimension, models.CompletedState)
			So(err, ShouldBeNil)
			So(eTag1, ShouldNotEqual, currentInstance.ETag)

			Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = "otherInstance"
				eTag2, err := newETagForBuildSearchTaskStateUpdate(instance2, dimension, models.CompletedState)
				So(err, ShouldBeNil)
				So(eTag2, ShouldNotEqual, eTag1)
			})

			Convey("Applying a different update to the same filter results in a different ETag", func() {
				eTag3, err := newETagForBuildSearchTaskStateUpdate(currentInstance, dimension, models.DetachedState)
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForNodeIDAndOrder(t *testing.T) {

	Convey("Given an instance", t, func() {

		currentInstance := testInstance()
		nodeID := "testNode"
		order := 2

		Convey("newETagForNodeIDAndOrder returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForNodeIDAndOrder(currentInstance, nodeID, &order)
			So(err, ShouldBeNil)
			So(eTag1, ShouldNotEqual, currentInstance.ETag)

			Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = "otherInstance"
				eTag2, err := newETagForNodeIDAndOrder(instance2, nodeID, &order)
				So(err, ShouldBeNil)
				So(eTag2, ShouldNotEqual, eTag1)
			})

			Convey("Applying a different update to the same filter results in a different ETag", func() {
				eTag3, err := newETagForNodeIDAndOrder(currentInstance, nodeID, nil)
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForAddDimensionOption(t *testing.T) {

	Convey("Given an instance", t, func() {

		currentInstance := testInstance()
		option := models.CachedDimensionOption{
			Code: "testCode",
			Name: "testName",
		}

		Convey("newETagForAddDimensionOption returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForAddDimensionOption(currentInstance, &option)
			So(err, ShouldBeNil)
			So(eTag1, ShouldNotEqual, currentInstance.ETag)

			Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = "otherInstance"
				eTag2, err := newETagForAddDimensionOption(instance2, &option)
				So(err, ShouldBeNil)
				So(eTag2, ShouldNotEqual, eTag1)
			})

			Convey("Applying a different update to the same filter results in a different ETag", func() {
				option := models.CachedDimensionOption{
					Code: "anotherCode",
					Name: "anotherName",
				}
				eTag3, err := newETagForAddDimensionOption(currentInstance, &option)
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})
		})
	})
}
