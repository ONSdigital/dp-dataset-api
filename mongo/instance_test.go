package mongo

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"go.mongodb.org/mongo-driver/bson"
	bsonprim "go.mongodb.org/mongo-driver/bson/primitive"
)

func TestSelector(t *testing.T) {
	Convey("Given some testing values to provide as selector paramters", t, func() {
		var testInstanceID = "instanceID"
		var testETag = "testETag"
		var testMongoTimestamp = bsonprim.Timestamp{T: 1234567890}

		Convey("Then, providing a zero timestamp and any eTag generates a selector that only queries by id", func() {
			s := selector(testInstanceID, bsonprim.Timestamp{}, AnyETag)
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
