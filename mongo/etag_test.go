package mongo

import (
	"strconv"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/smartystreets/goconvey/convey"
)

func instanceID(number int) string {
	return "instanceID" + strconv.Itoa(number)
}

func testInstance() *models.Instance {
	i := &models.Instance{
		CollectionID: "testCollection",
		Dimensions:   []models.Dimension{{Name: "dim1"}, {Name: "dim2"}},
		Edition:      "testEdition",
		InstanceID:   instanceID(1),
		State:        models.CreatedState,
	}
	eTag0, err := i.Hash(nil)
	convey.So(err, convey.ShouldBeNil)
	i.ETag = eTag0
	return i
}

func testVersion() *models.Version {
	v := &models.Version{
		CollectionID: "testCollection",
		Dimensions:   []models.Dimension{{Name: "dim1"}, {Name: "dim2"}},
		Downloads: &models.DownloadList{
			CSV: &models.DownloadObject{
				HRef:    "download.service/file.csv",
				Private: "private_s3/file.csv",
			},
		},
		Edition: "testEdition",
		ID:      "123",
		State:   models.CreatedState,
	}
	eTag0, err := v.Hash(nil)
	convey.So(err, convey.ShouldBeNil)
	v.ETag = eTag0
	return v
}

func TestNewETagForUpdate(t *testing.T) {
	convey.Convey("Given an instance", t, func() {
		currentInstance := testInstance()

		update := &models.Instance{
			State: models.CompletedState,
		}

		convey.Convey("getNewETagForUpdate returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForUpdate(currentInstance, update)
			convey.So(err, convey.ShouldBeNil)
			convey.So(eTag1, convey.ShouldNotEqual, currentInstance.ETag)

			convey.Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = instanceID(2)
				eTag2, err := newETagForUpdate(instance2, update)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag2, convey.ShouldNotEqual, eTag1)
			})

			convey.Convey("Applying a different update to the same instance results in a different ETag", func() {
				update2 := &models.Instance{
					InstanceID: instanceID(3),
				}
				eTag3, err := newETagForUpdate(currentInstance, update2)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag3, convey.ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForVersionUpdate(t *testing.T) {
	convey.Convey("Given a version", t, func() {
		currentVersion := testVersion()

		update := &models.Version{
			State: models.CompletedState,
		}

		convey.Convey("newETagForVersionUpdate returns an eTag that is different from the original version ETag", func() {
			eTag1, err := newETagForVersionUpdate(currentVersion, update)
			convey.So(err, convey.ShouldBeNil)
			convey.So(eTag1, convey.ShouldNotEqual, currentVersion.ETag)

			convey.Convey("Applying the same update to a different version results in a different ETag", func() {
				v2 := testVersion()
				v2.ID = "otherVersion"
				eTag2, err := newETagForVersionUpdate(v2, update)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag2, convey.ShouldNotEqual, eTag1)
			})

			convey.Convey("Applying a different update to the same version results in a different ETag", func() {
				update2 := &models.Version{
					ID: "anotherInstanceID",
				}
				eTag3, err := newETagForVersionUpdate(currentVersion, update2)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag3, convey.ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForAddEvent(t *testing.T) {
	convey.Convey("Given an instance", t, func() {
		currentInstance := testInstance()

		event := models.Event{
			Message: "testEvent",
		}

		convey.Convey("newETagForAddEvent returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForAddEvent(currentInstance, &event)
			convey.So(err, convey.ShouldBeNil)
			convey.So(eTag1, convey.ShouldNotEqual, currentInstance.ETag)

			convey.Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = instanceID(2)
				eTag2, err := newETagForAddEvent(instance2, &event)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag2, convey.ShouldNotEqual, eTag1)
			})

			convey.Convey("Applying a different update to the same instance results in a different ETag", func() {
				event = models.Event{
					Message: "anotherEvent",
				}
				eTag3, err := newETagForAddEvent(currentInstance, &event)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag3, convey.ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForObservationsInserted(t *testing.T) {
	convey.Convey("Given an instance", t, func() {
		currentInstance := testInstance()

		var obsInserted int64 = 12345

		convey.Convey("newETagForObservationsInserted returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForObservationsInserted(currentInstance, obsInserted)
			convey.So(err, convey.ShouldBeNil)
			convey.So(eTag1, convey.ShouldNotEqual, currentInstance.ETag)

			convey.Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = instanceID(2)
				eTag2, err := newETagForObservationsInserted(instance2, obsInserted)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag2, convey.ShouldNotEqual, eTag1)
			})

			convey.Convey("Applying a different update to the same instance results in a different ETag", func() {
				obsInserted = 54321
				eTag3, err := newETagForObservationsInserted(currentInstance, obsInserted)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag3, convey.ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForStateUpdate(t *testing.T) {
	convey.Convey("Given an instance", t, func() {
		currentInstance := testInstance()

		convey.Convey("newETagForStateUpdate returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForStateUpdate(currentInstance, models.CompletedState)
			convey.So(err, convey.ShouldBeNil)
			convey.So(eTag1, convey.ShouldNotEqual, currentInstance.ETag)

			convey.Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = instanceID(2)
				eTag2, err := newETagForStateUpdate(instance2, models.CompletedState)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag2, convey.ShouldNotEqual, eTag1)
			})

			convey.Convey("Applying a different update to the same instance results in a different ETag", func() {
				eTag3, err := newETagForStateUpdate(currentInstance, models.DetachedState)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag3, convey.ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForHierarchyTaskStateUpdate(t *testing.T) {
	convey.Convey("Given an instance", t, func() {
		currentInstance := testInstance()
		dimension := "dim1"

		convey.Convey("newETagForHierarchyTaskStateUpdate returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForHierarchyTaskStateUpdate(currentInstance, dimension, models.CompletedState)
			convey.So(err, convey.ShouldBeNil)
			convey.So(eTag1, convey.ShouldNotEqual, currentInstance.ETag)

			convey.Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = instanceID(2)
				eTag2, err := newETagForHierarchyTaskStateUpdate(instance2, dimension, models.CompletedState)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag2, convey.ShouldNotEqual, eTag1)
			})

			convey.Convey("Applying a different update to the same instance results in a different ETag", func() {
				eTag3, err := newETagForHierarchyTaskStateUpdate(currentInstance, dimension, models.DetachedState)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag3, convey.ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForBuildSearchTaskStateUpdate(t *testing.T) {
	convey.Convey("Given an instance", t, func() {
		currentInstance := testInstance()
		dimension := "dim1"

		convey.Convey("newETagForBuildSearchTaskStateUpdate returns an eTag that is different from the original instance ETag", func() {
			eTag1, err := newETagForBuildSearchTaskStateUpdate(currentInstance, dimension, models.CompletedState)
			convey.So(err, convey.ShouldBeNil)
			convey.So(eTag1, convey.ShouldNotEqual, currentInstance.ETag)

			convey.Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = instanceID(2)
				eTag2, err := newETagForBuildSearchTaskStateUpdate(instance2, dimension, models.CompletedState)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag2, convey.ShouldNotEqual, eTag1)
			})

			convey.Convey("Applying a different update to the same instance results in a different ETag", func() {
				eTag3, err := newETagForBuildSearchTaskStateUpdate(currentInstance, dimension, models.DetachedState)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag3, convey.ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForOptions(t *testing.T) {
	convey.Convey("Given an instance", t, func() {
		currentInstance := testInstance()

		optionUpsert := models.CachedDimensionOption{
			Code: "testCode",
			Name: "testName",
		}

		anotherOptionUpsert := models.CachedDimensionOption{
			Code: "anotherCode",
			Name: "anotherName",
		}

		ord := 6
		optionUpdate := models.DimensionOption{
			NodeID: "myNode",
			Order:  &ord,
		}

		anotherOptionUpdate := models.DimensionOption{
			NodeID: "anotherNodeID",
		}

		convey.Convey("newETagForOptions returns an eTag that is different from the original instance ETag when it is provided an upsert", func() {
			eTag1, err := newETagForOptions(currentInstance, []*models.CachedDimensionOption{&optionUpsert}, nil)
			convey.So(err, convey.ShouldBeNil)
			convey.So(eTag1, convey.ShouldNotEqual, currentInstance.ETag)

			convey.Convey("Applying the same upsert to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = instanceID(2)
				eTag2, err := newETagForOptions(instance2, []*models.CachedDimensionOption{&optionUpsert}, nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag2, convey.ShouldNotEqual, eTag1)
			})

			convey.Convey("Applying a different upsert to the same instance results in a different ETag", func() {
				eTag3, err := newETagForOptions(currentInstance, []*models.CachedDimensionOption{&anotherOptionUpsert}, nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag3, convey.ShouldNotEqual, eTag1)
			})

			convey.Convey("Applying an extra update to the same instance with the same update results in a different ETag", func() {
				eTag3, err := newETagForOptions(currentInstance, []*models.CachedDimensionOption{&optionUpsert}, []*models.DimensionOption{&optionUpdate})
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag3, convey.ShouldNotEqual, eTag1)
			})

			convey.Convey("Applying an upsert to the same instance containing an extra dimensions results in a different ETag", func() {
				option := models.CachedDimensionOption{
					Code: "anotherCode",
					Name: "anotherName",
				}
				eTag3, err := newETagForOptions(currentInstance, []*models.CachedDimensionOption{&option, &anotherOptionUpsert}, nil)
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag3, convey.ShouldNotEqual, eTag1)
			})
		})

		convey.Convey("newETagForOptions returns an eTag that is different from the original instance ETag when it is provided an update", func() {
			eTag1, err := newETagForOptions(currentInstance, nil, []*models.DimensionOption{&optionUpdate})
			convey.So(err, convey.ShouldBeNil)
			convey.So(eTag1, convey.ShouldNotEqual, currentInstance.ETag)

			convey.Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = instanceID(2)
				eTag2, err := newETagForOptions(instance2, nil, []*models.DimensionOption{&optionUpdate})
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag2, convey.ShouldNotEqual, eTag1)
			})

			convey.Convey("Applying a different update to the same instance results in a different ETag", func() {
				eTag3, err := newETagForOptions(currentInstance, nil, []*models.DimensionOption{&anotherOptionUpdate})
				convey.So(err, convey.ShouldBeNil)
				convey.So(eTag3, convey.ShouldNotEqual, eTag1)
			})
		})
	})
}
