package instance

import (
	"fmt"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

// Reader used to trigger errors on reading
type Reader struct {
}

// Read a function used to mock errors
func (f Reader) Read(bytes []byte) (int, error) {
	return 0, fmt.Errorf("Reader failed")
}

func TestUnmarshalInstanceWithBadReader(t *testing.T) {
	Convey("Create an instance with an invalid reader", t, func() {
		instance, err := unmarshalInstance(Reader{}, true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, "Failed to read message body")
	})
}

func TestUnmarshalInstanceWithInvalidJson(t *testing.T) {
	Convey("Create an instance with invalid json", t, func() {
		instance, err := unmarshalInstance(strings.NewReader("{ "), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldContainSubstring, "Failed to parse json body")
	})
}

func TestUnmarshalInstanceWithEmptyJson(t *testing.T) {
	Convey("Create an instance with empty json", t, func() {
		instance, err := unmarshalInstance(strings.NewReader("{ }"), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, "Missing job properties")
	})

	Convey("Create an instance with empty job link", t, func() {
		instance, err := unmarshalInstance(strings.NewReader(`{"links":{"job": null}}`), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, "Missing job properties")
	})

	Convey("Create an instance with empty href in job link", t, func() {
		instance, err := unmarshalInstance(strings.NewReader(`{"links":{"job":{"id": "456"}}}`), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, "Missing job properties")
	})

	Convey("Create an instance with empty href in job link", t, func() {
		instance, err := unmarshalInstance(strings.NewReader(`{"links":{"job":{"href": "http://localhost:21800/jobs/456"}}}`), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, "Missing job properties")
	})

	Convey("Update an instance with empty json", t, func() {
		instance, err := unmarshalInstance(strings.NewReader("{ }"), false)
		So(instance, ShouldNotBeEmpty)
		So(err, ShouldBeNil)
	})
}

func TestUnmarshalInstanceWithMissingFields(t *testing.T) {
	Convey("Create an instance with no id", t, func() {
		instance, err := unmarshalInstance(strings.NewReader(`{"links": { "job": { "link":"http://localhost:2200/jobs/123-456" } }}`), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, "Missing job properties")
	})

	Convey("Create an instance with no link", t, func() {
		instance, err := unmarshalInstance(strings.NewReader(`{"links": { "job": {"id":"123-456"} }}`), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, "Missing job properties")
	})

	Convey("Update an instance with no id", t, func() {
		instance, err := unmarshalInstance(strings.NewReader(`{"links": { "job": { "link":"http://localhost:2200/jobs/123-456" } }}`), false)
		So(instance, ShouldNotBeNil)
		So(err, ShouldBeNil)
	})

	Convey("Update an instance with no link", t, func() {
		instance, err := unmarshalInstance(strings.NewReader(`{"links": { "job": {"id":"123-456"} }}`), false)
		So(instance, ShouldNotBeNil)
		So(err, ShouldBeNil)
	})
}

func TestUnmarshalInstance(t *testing.T) {
	Convey("Create an instance with the required fields", t, func() {
		instance, err := unmarshalInstance(strings.NewReader(`{"links": { "job": { "id":"123-456", "href":"http://localhost:2200/jobs/123-456" } }}`), true)
		So(err, ShouldBeNil)
		So(instance.Links.Job.ID, ShouldEqual, "123-456")
	})
}

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
