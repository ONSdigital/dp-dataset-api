package mongo

import (
	"testing"

	"github.com/globalsign/mgo/bson"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSelector(t *testing.T) {

	Convey("Given some testing values to provide as selector paramters", t, func() {
		var testInstanceID string = "instanceID"
		var testETag string = "testETag"
		var testMongoTimestamp bson.MongoTimestamp = 1234567890

		Convey("Then, providing a zero timestamp and any eTag generates a selector that only queries by id", func() {
			s := selector(testInstanceID, 0, AnyETag)
			So(s, ShouldResemble, bson.M{"id": testInstanceID})
		})

		Convey("Then, providing values for timestamp, and eTag generates a selector that queries by filterID, timestamp and eTag", func() {
			s := selector(testInstanceID, testMongoTimestamp, testETag)
			So(s, ShouldResemble, bson.M{
				"id":               testInstanceID,
				"unique_timestamp": testMongoTimestamp,
				"e_tag":            testETag,
			})
		})
	})
}
