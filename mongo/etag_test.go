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
	So(err, ShouldBeNil)
	v.ETag = eTag0
	return v
}

func testDataset() *models.DatasetUpdate {
	d := &models.DatasetUpdate{
		ID: "testDatasetCollectionID",
		Next: &models.Dataset{
			State:       models.CreatedState,
			License:     "a dataset license for testing",
			Title:       "a dataset title for testing",
			Description: "some dataset description for testing",
			QMI: &models.GeneralDetails{
				Description: "some qmi description for testing",
				HRef:        "http://localhost:22000/datasets/href",
				Title:       "some qmi title for testing",
			},
			Type: "filterable",
		},
	}
	eTag0, err := d.Hash(nil)
	So(err, ShouldBeNil)
	d.ETag = eTag0
	return d
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

			Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = "otherInstance"
				eTag2, err := newETagForUpdate(instance2, update)
				So(err, ShouldBeNil)
				So(eTag2, ShouldNotEqual, eTag1)
			})

			Convey("Applying a different update to the same instance results in a different ETag", func() {
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

func TestNewETagForVersionUpdate(t *testing.T) {
	Convey("Given a version", t, func() {

		currentVersion := testVersion()

		update := &models.Version{
			State: models.CompletedState,
		}

		Convey("newETagForVersionUpdate returns an eTag that is different from the original version ETag", func() {
			eTag1, err := newETagForVersionUpdate(currentVersion, update)
			So(err, ShouldBeNil)
			So(eTag1, ShouldNotEqual, currentVersion.ETag)

			Convey("Applying the same update to a different version results in a different ETag", func() {
				v2 := testVersion()
				v2.ID = "otherVersion"
				eTag2, err := newETagForVersionUpdate(v2, update)
				So(err, ShouldBeNil)
				So(eTag2, ShouldNotEqual, eTag1)
			})

			Convey("Applying a different update to the same version results in a different ETag", func() {
				update2 := &models.Version{
					ID: "anotherInstanceID",
				}
				eTag3, err := newETagForVersionUpdate(currentVersion, update2)
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

			Convey("Applying a different update to the same instance results in a different ETag", func() {
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

			Convey("Applying a different update to the same instance results in a different ETag", func() {
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

			Convey("Applying a different update to the same instance results in a different ETag", func() {
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

			Convey("Applying a different update to the same instance results in a different ETag", func() {
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

			Convey("Applying a different update to the same instance results in a different ETag", func() {
				eTag3, err := newETagForBuildSearchTaskStateUpdate(currentInstance, dimension, models.DetachedState)
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForOptions(t *testing.T) {

	Convey("Given an instance", t, func() {

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

		Convey("newETagForOptions returns an eTag that is different from the original instance ETag when it is provided an upsert", func() {
			eTag1, err := newETagForOptions(currentInstance, []*models.CachedDimensionOption{&optionUpsert}, nil)
			So(err, ShouldBeNil)
			So(eTag1, ShouldNotEqual, currentInstance.ETag)

			Convey("Applying the same upsert to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = "otherInstance"
				eTag2, err := newETagForOptions(instance2, []*models.CachedDimensionOption{&optionUpsert}, nil)
				So(err, ShouldBeNil)
				So(eTag2, ShouldNotEqual, eTag1)
			})

			Convey("Applying a different upsert to the same instance results in a different ETag", func() {
				eTag3, err := newETagForOptions(currentInstance, []*models.CachedDimensionOption{&anotherOptionUpsert}, nil)
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})

			Convey("Applying an extra update to the same instance with the same update results in a different ETag", func() {
				eTag3, err := newETagForOptions(currentInstance, []*models.CachedDimensionOption{&optionUpsert}, []*models.DimensionOption{&optionUpdate})
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})

			Convey("Applying an upsert to the same instance containing an extra dimensions results in a different ETag", func() {
				option := models.CachedDimensionOption{
					Code: "anotherCode",
					Name: "anotherName",
				}
				eTag3, err := newETagForOptions(currentInstance, []*models.CachedDimensionOption{&option, &anotherOptionUpsert}, nil)
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})
		})

		Convey("newETagForOptions returns an eTag that is different from the original instance ETag when it is provided an update", func() {
			eTag1, err := newETagForOptions(currentInstance, nil, []*models.DimensionOption{&optionUpdate})
			So(err, ShouldBeNil)
			So(eTag1, ShouldNotEqual, currentInstance.ETag)

			Convey("Applying the same update to a different instance results in a different ETag", func() {
				instance2 := testInstance()
				instance2.InstanceID = "otherInstance"
				eTag2, err := newETagForOptions(instance2, nil, []*models.DimensionOption{&optionUpdate})
				So(err, ShouldBeNil)
				So(eTag2, ShouldNotEqual, eTag1)
			})

			Convey("Applying a different update to the same instance results in a different ETag", func() {
				eTag3, err := newETagForOptions(currentInstance, nil, []*models.DimensionOption{&anotherOptionUpdate})
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})
		})
	})
}

func TestNewETagForDatasetUpdate(t *testing.T) {
	Convey("Given a dataset", t, func() {

		currentDataset := testDataset()

		update := &models.Dataset{
			ID:           "testDatasetCollectionID",
			State:        models.AssociatedState,
			CollectionID: "testCollectionID",
			Title:        "an updated dataset title for testing",
		}
		Convey("newETagForDatasetUpdate returns an eTag that is different from the original dataset ETag", func() {
			eTag1, err := newETagForDatasetUpdate(currentDataset, update)
			So(err, ShouldBeNil)
			So(eTag1, ShouldNotEqual, currentDataset.ETag)

			Convey("Applying the same update to a different dataset results in a different ETag", func() {
				d2 := testDataset()
				d2.ID = "otherTestDatasetCollectionID"
				eTag2, err := newETagForDatasetUpdate(d2, update)
				So(err, ShouldBeNil)
				So(eTag2, ShouldNotEqual, eTag1)
			})

			Convey("Applying a different update to the same dataset results in a different ETag", func() {
				update2 := &models.Dataset{
					ID:           "testDatasetCollectionID",
					State:        models.AssociatedState,
					CollectionID: "testCollectionID",
					Title:        "a second update of the dataset title for testing",
				}
				eTag3, err := newETagForDatasetUpdate(currentDataset, update2)
				So(err, ShouldBeNil)
				So(eTag3, ShouldNotEqual, eTag1)
			})
		})
	})
}
