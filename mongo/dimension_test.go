package mongo

import (
	"errors"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSafeUpsert(t *testing.T) {
	t.Parallel()

	Convey("Given a valid collection, bulk and option", t, func() {
		c := mgo.Collection{FullName: "CollectionName"}
		bulk := c.Bulk()
		option := models.DimensionOption{InstanceID: "123", Option: "op1", Name: "Test Option", Label: "Test Label"}

		Convey("When a valid Upsert is performed via Safe Upsert", func() {
			err := SafeUpsert(
				bulk,
				bson.M{"instance_id": option.InstanceID, "name": option.Name, "option": option.Option},
				&option,
			)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When a SafeUpsert is performed with the wrong nubmer of paramters", func() {
			option := models.DimensionOption{InstanceID: "123", Option: "op1", Name: "Test Option", Label: "Test Label"}
			err := SafeUpsert(
				bulk,
				bson.M{"instance_id": option.InstanceID, "name": option.Name, "option": option.Option},
			)

			Convey("Then the expected error is returned instead of panicking", func() {
				So(err, ShouldResemble, errors.New("upsert panicked"))
			})
		})
	})
}
