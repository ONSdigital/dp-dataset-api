package mongo

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"

	"go.mongodb.org/mongo-driver/bson"
	bsonprim "go.mongodb.org/mongo-driver/bson/primitive"
)

func TestSelector(t *testing.T) {
	convey.Convey("Given some testing values to provide as selector paramters", t, func() {
		var testInstanceID = "instanceID"
		var testETag = "testETag"
		var testMongoTimestamp = bsonprim.Timestamp{T: 1234567890}

		convey.Convey("Then, providing a zero timestamp and any eTag generates a selector that only queries by id", func() {
			s := selector(testInstanceID, bsonprim.Timestamp{}, AnyETag)
			convey.So(s, convey.ShouldResemble, bson.M{"id": testInstanceID})
		})

		convey.Convey("Then, providing values for timestamp, and eTag generates a selector that queries by filterID, timestamp and eTag", func() {
			s := selector(testInstanceID, testMongoTimestamp, testETag)
			convey.So(s, convey.ShouldResemble, bson.M{
				"id":               testInstanceID,
				"unique_timestamp": testMongoTimestamp,
				"e_tag":            testETag,
			})
		})
	})
}
