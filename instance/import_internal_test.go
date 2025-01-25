package instance

import (
	"strings"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestUnmarshalImportTaskWithBadReader(t *testing.T) {
	convey.Convey("Create an import task with an invalid reader", t, func() {
		task, err := unmarshalImportTasks(Reader{})
		convey.So(task, convey.ShouldBeNil)
		convey.So(err.Error(), convey.ShouldEqual, "failed to read message body")
	})
}

func TestUnmarshalImportTaskWithInvalidJson(t *testing.T) {
	convey.Convey("Create an import observation task with invalid json", t, func() {
		task, err := unmarshalImportTasks(strings.NewReader("{ "))
		convey.So(task, convey.ShouldBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "unexpected end of JSON input")
	})
}

func TestUnmarshalImportTaskWithInvalidData(t *testing.T) {
	convey.Convey("Create an import observation task with correctly named fields of the wrong type", t, func() {
		task, err := unmarshalImportTasks(strings.NewReader(`{"build_hierarchies": "this should fail"}`))
		convey.So(task, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, "json: cannot unmarshal string into Go struct field InstanceImportTasks.build_hierarchies of type []*models.BuildHierarchyTask")
	})
}

func TestUnmarshalImportTask_ImportObservations(t *testing.T) {
	convey.Convey("Create an import observation task with valid json", t, func() {
		task, err := unmarshalImportTasks(strings.NewReader(`{"import_observations":{"state":"completed"}}`))
		convey.So(err, convey.ShouldBeNil)
		convey.So(task, convey.ShouldNotBeNil)
		convey.So(task.ImportObservations, convey.ShouldNotBeNil)
		convey.So(task.ImportObservations.State, convey.ShouldEqual, "completed")
	})
}

func TestUnmarshalImportTask_BuildHierarchies(t *testing.T) {
	convey.Convey("Create an import observation task with valid json", t, func() {
		task, err := unmarshalImportTasks(strings.NewReader(`{"build_hierarchies":[{"state":"completed"}]}`))
		convey.So(err, convey.ShouldBeNil)
		convey.So(task, convey.ShouldNotBeNil)
		convey.So(task.BuildHierarchyTasks, convey.ShouldNotBeNil)
		convey.So(task.BuildHierarchyTasks[0].State, convey.ShouldEqual, "completed")
	})
}

func TestUnmarshalImportTask_BuildSearch(t *testing.T) {
	convey.Convey("Create an import observation task with valid json", t, func() {
		task, err := unmarshalImportTasks(strings.NewReader(`{"build_search_indexes":[{"state":"completed"}]}`))
		convey.So(err, convey.ShouldBeNil)
		convey.So(task, convey.ShouldNotBeNil)
		convey.So(task.BuildSearchIndexTasks, convey.ShouldNotBeNil)
		convey.So(task.BuildSearchIndexTasks[0].State, convey.ShouldEqual, "completed")
	})
}
