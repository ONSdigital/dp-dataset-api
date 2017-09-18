package mongo

import (
	"testing"

	"gopkg.in/mgo.v2/bson"

	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	id        = "123"
	editionID = "2017"
	state     = "published"
	versionID = 2
)

func TestBuildEditionsQuery(t *testing.T) {
	t.Parallel()
	Convey("When no state was set", t, func() {

		expectedSelector := bson.M{
			"links.dataset.id": id,
		}

		selector := buildEditionsQuery(id, "")
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})

	Convey("When state was set to published", t, func() {

		expectedSelector := bson.M{
			"links.dataset.id": id,
			"state":            state,
		}

		selector := buildEditionsQuery(id, state)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})
}

func TestBuildEditionQuery(t *testing.T) {
	t.Parallel()
	Convey("When no state was set", t, func() {

		expectedSelector := bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
		}

		selector := buildEditionQuery(id, editionID, "")
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})

	Convey("When state was set to published", t, func() {

		expectedSelector := bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"state":            state,
		}

		selector := buildEditionQuery(id, editionID, state)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})
}

func TestBuildVersionsQuery(t *testing.T) {
	t.Parallel()
	Convey("When no state was set", t, func() {

		expectedSelector := bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
		}

		selector := buildVersionsQuery(id, editionID, "")
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})

	Convey("When state was set to published", t, func() {

		expectedSelector := bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"state":            state,
		}

		selector := buildVersionsQuery(id, editionID, state)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})
}

func TestBuildVersionQuery(t *testing.T) {
	t.Parallel()
	Convey("When no state was set", t, func() {

		expectedSelector := bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"version":          versionID,
		}

		selector := buildVersionQuery(id, editionID, "", versionID)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})

	Convey("When state was set to published", t, func() {

		expectedSelector := bson.M{
			"links.dataset.id": id,
			"edition":          editionID,
			"version":          versionID,
			"state":            state,
		}

		selector := buildVersionQuery(id, editionID, state, versionID)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})
}

func TestDatasetUpdateQuery(t *testing.T) {
	t.Parallel()
	Convey("When all possible fields exist", t, func() {

		expectedUpdate := bson.M{
			"next.collection_id":     "12345678",
			"next.contact.email":     "njarrod@test.com",
			"next.contact.name":      "natalie jarrod",
			"next.contact.telephone": "01658 234567",
			"next.description":       "test description",
			"next.next_release":      "2018-05-05",
			"next.periodicity":       "yearly",
			"next.publisher.href":    "http://ons.gov.uk",
			"next.publisher.name":    "Office of National Statistics",
			"next.publisher.type":    "Public",
			"next.theme":             "construction",
			"next.title":             "CPI",
		}

		dataset := &models.Dataset{
			Contact: models.ContactDetails{
				Email:     "njarrod@test.com",
				Name:      "natalie jarrod",
				Telephone: "01658 234567",
			},
			CollectionID: "12345678",
			Description:  "test description",
			NextRelease:  "2018-05-05",
			Periodicity:  "yearly",
			Publisher: models.Publisher{
				Name: "Office of National Statistics",
				Type: "Public",
				HRef: "http://ons.gov.uk",
			},
			Theme: "construction",
			Title: "CPI",
		}

		selector := createDatasetUpdateQuery(dataset)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedUpdate)
	})
}

func TestVersionUpdateQuery(t *testing.T) {
	t.Parallel()
	Convey("When all possible fields exist", t, func() {

		expectedUpdate := bson.M{
			"collection_id": "12345678",
			"instance_id":   "87654321",
			"license":       "ONS License",
			"release_date":  "2017-09-09",
			"state":         "published",
		}

		version := &models.Version{
			CollectionID: "12345678",
			InstanceID:   "87654321",
			License:      "ONS License",
			ReleaseDate:  "2017-09-09",
			State:        "published",
		}

		selector := createVersionUpdateQuery(version)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedUpdate)
	})
}
