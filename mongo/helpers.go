package mongo

import (
	mongodriver "github.com/ONSdigital/dp-mongodb/v3/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	bsonprim "go.mongodb.org/mongo-driver/bson/primitive"
)

// datasetSelector creates a select query for mongoDB with the provided parameters
// - documentID represents the ID of the document that we want to query. Required.
// - timestamp is a unique MongoDB timestamp to be matched to prevent race conditions. Optional.
// - eTagselector is a unique hash of a document to be matched to prevent race conditions. Optional.
func datasetSelector(documentID string, timestamp bsonprim.Timestamp, eTagSelector string) bson.M {

	selector := bson.M{"_id": documentID}
	if !timestamp.IsZero() {
		selector[mongodriver.UniqueTimestampKey] = timestamp
	}
	if eTagSelector != AnyETag {
		selector["e_tag"] = eTagSelector
	}
	return selector
}
