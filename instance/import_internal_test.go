package instance

import (
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestUnmarshalImportTaskWithBadReader(t *testing.T) {
	Convey("Create an import task with an invalid reader", t, func() {
		task, err := unmarshalImportTasks(Reader{})
		So(task, ShouldBeNil)
		So(err.Error(), ShouldEqual, "failed to read message body")
	})
}

func TestUnmarshalImportTaskWithInvalidJson(t *testing.T) {
	Convey("Create an import observation task with invalid json", t, func() {
		task, err := unmarshalImportTasks(strings.NewReader("{ "))
		So(task, ShouldBeNil)
		So(err.Error(), ShouldContainSubstring, "failed to parse json body")
	})
}

func TestUnmarshalImportTask_ImportObservations(t *testing.T) {
	Convey("Create an import observation task with valid json", t, func() {
		task, err := unmarshalImportTasks(strings.NewReader(`{"import_observations":{"state":"completed"}}`))
		So(err, ShouldBeNil)
		So(task, ShouldNotBeNil)
		So(task.ImportObservations, ShouldNotBeNil)
		So(task.ImportObservations.State, ShouldEqual, "completed")
	})
}

func TestUnmarshalImportTask_BuildHierarchies(t *testing.T) {
	Convey("Create an import observation task with valid json", t, func() {
		task, err := unmarshalImportTasks(strings.NewReader(`{"build_hierarchies":[{"state":"completed"}]}`))
		So(err, ShouldBeNil)
		So(task, ShouldNotBeNil)
		So(task.BuildHierarchyTasks, ShouldNotBeNil)
		So(task.BuildHierarchyTasks[0].State, ShouldEqual, "completed")
	})
}

func TestUnmarshalImportTask_BuildSearch(t *testing.T) {
	Convey("Create an import observation task with valid json", t, func() {
		task, err := unmarshalImportTasks(strings.NewReader(`{"build_search_indexes":[{"state":"completed"}]}`))
		So(err, ShouldBeNil)
		So(task, ShouldNotBeNil)
		So(task.BuildSearchIndexTasks, ShouldNotBeNil)
		So(task.BuildSearchIndexTasks[0].State, ShouldEqual, "completed")
	})
}
