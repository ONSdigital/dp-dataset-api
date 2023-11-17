package mongo

import (
	"github.com/ONSdigital/dp-dataset-api/models"
	"go.mongodb.org/mongo-driver/bson"
)

/*
db.getCollection('instances').aggregate([
    { $match: {"links.dataset.id": "weekly-deaths-local-authority", "state": "published"} },
    { $sort: {"edition": 1, "version": 1}},
    { $group : {_id: '$edition', "version": {$last: "$version"}, "updated": {$last: "$release_date"}, , "doc": {$last: "$$CURRENT"}}}
  ])

  Query all instances to those published for a given dataset ID
  Sort the editions alphabetically, and the versions numerically
  Return the latest edition ID, version ID, last_update/published date and the whole 'instance' document as 'doc'

  This should work for:
  - embedding in /datasets/id
  - returning the latest version at /editions and /editions/id

  For /versions the edition selection can move to the $match stage and theres no longer a need for grouping
  For /versions/id all elements move to the $match stage
*/

// get the embedded resources needed on a dataset response - mapping to the `DatasetEmbedded` struct
func buildDatasetEmbeddedQuery(id, state string, authorised bool) []bson.M {
	selector := selectByDatasetLinkAndState(id, state, authorised)
	sort := sortByEditionThenVersion()

	group := bson.M{
		"$group": bson.M{
			"_id":    "$edition",
			"issued": bson.M{"$last": "$release_date"}, //TODO: this should potentially be 'last_updated' not 'release_date'
			//	"doc":    "$$CURRENT",
		},
	}

	return []bson.M{selector, sort, group}
}

func buildLatestEditionAndVersionQuery(id, edition, state string, authorised bool) []bson.M {

	selector := selectByDatasetLinkAndState(id, state, authorised)
	if edition != "" {
		selector = selectByEditionAndState(id, edition, state, authorised)
	}
	sort := sortByEditionThenVersion()

	group := bson.M{
		"$group": bson.M{
			"_id":          "$edition",
			"version":      bson.M{"$last": "$version"},
			"release_date": bson.M{"$last": "$release_date"}, //TODO: this should potentially be 'last_updated' not 'release_date'
			"document":     bson.M{"$last": "$$CURRENT"},
		},
	}

	replace := bson.M{
		"$replaceRoot": bson.M{
			"newRoot": "$document",
		},
	}
	return []bson.M{selector, sort, group, replace}

}

func selectByDatasetLinkAndState(id, state string, authorised bool) bson.M {
	if !authorised {
		state = models.PublishedState
	}

	// all queries must get the dataset by id
	selector := bson.M{
		"$match": bson.M{"_links.dataset.id": id},
	}

	if state != "" {
		selector = bson.M{
			"$match": bson.M{"_links.dataset.id": id, "state": state},
		}
	}

	return selector
}

func selectByEditionAndState(id, edition, state string, authorised bool) bson.M {
	if !authorised {
		state = models.PublishedState
	}

	// all queries must get the dataset by id
	selector := bson.M{
		"$match": bson.M{"_links.dataset.id": id, "edition": edition},
	}

	if state != "" {
		selector = bson.M{
			"$match": bson.M{"_links.dataset.id": id, "edition": edition, "state": state},
		}
	}

	return selector
}

func sortByEditionThenVersion() bson.M {
	return bson.M{
		"$sort": bson.M{
			"edition": 1,
			"version": 1,
		},
	}
}
