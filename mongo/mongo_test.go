package mongo

import (
	"testing"

	"gopkg.in/mgo.v2/bson"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	id        = "123"
	editionID = "2017"
	state     = "published"
	versionID = "2"
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

		selector := buildVersionQuery(id, editionID, versionID, "")
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

		selector := buildVersionQuery(id, editionID, versionID, state)
		So(selector, ShouldNotBeNil)
		So(selector, ShouldResemble, expectedSelector)
	})
}
