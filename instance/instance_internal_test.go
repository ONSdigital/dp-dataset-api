package instance

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/models"
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

	Convey("Update an instance with empty json", t, func() {
		instance, err := unmarshalInstance(strings.NewReader("{ }"), false)
		So(instance, ShouldResemble, &models.Instance{})
		So(err, ShouldBeNil)
	})
}

func TestUnmarshalInstanceWithMissingFields(t *testing.T) {
	Convey("Create an instance with no id", t, func() {
		instance, err := unmarshalInstance(strings.NewReader(`{"job": { "link":"http://localhost:2200/jobs/123-456" } }`), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, "Missing job properties")
	})

	Convey("Create an instance with no link", t, func() {
		instance, err := unmarshalInstance(strings.NewReader(`{"job": {"id":"123-456"} }`), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, "Missing job properties")
	})

	Convey("Update an instance with no id", t, func() {
		instance, err := unmarshalInstance(strings.NewReader(`{"job": { "link":"http://localhost:2200/jobs/123-456" } }`), false)
		So(instance, ShouldNotBeNil)
		So(err, ShouldBeNil)
	})

	Convey("Update an instance with no link", t, func() {
		instance, err := unmarshalInstance(strings.NewReader(`{"job": {"id":"123-456"} }`), false)
		So(instance, ShouldNotBeNil)
		So(err, ShouldBeNil)
	})
}

func TestUnmarshalInstance(t *testing.T) {
	Convey("Create an instance with the required fields", t, func() {
		instance, err := unmarshalInstance(strings.NewReader(`{"job": { "id":"123-456", "link":"http://localhost:2200/jobs/123-456" } }`), true)
		So(err, ShouldBeNil)
		So(instance.Job.ID, ShouldEqual, "123-456")
	})
}
