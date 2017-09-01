package models

import (
	. "github.com/smartystreets/goconvey/convey"
	"strings"
	"testing"
	"fmt"
)

// Reader used to trigger errors on reading
type Reader struct {
}

// Read a function used to mock errors
func (f Reader) Read(bytes []byte) (int, error) {
	return 0, fmt.Errorf("Reader failed")
}

func TestCreateInstanceWithBadReader(t *testing.T) {
	Convey("Create an instance with an invalid reader", t, func() {
		_, err := CreateInstance(Reader{})
		So(err, ShouldNotBeNil)
	})
}

func TestCreateInstanceWithEmptyJson(t *testing.T) {
	Convey("Create an instance with empty json", t, func() {
		instance, err := CreateInstance(strings.NewReader("{ }"))
		So(err, ShouldBeNil)
		So(instance.Defaults(), ShouldNotBeNil)
	})
}

func TestCreateInstance(t *testing.T) {
	Convey("Create an instance with the required fields", t, func() {
		instance, err := CreateInstance(strings.NewReader(`{"job": { "id":"123-456", "link":"http://localhost:2200/jobs/123-456" } }`))
		So(err, ShouldBeNil)
		So(instance.Defaults(), ShouldBeNil)
		So(instance.Job.ID, ShouldEqual, "123-456")
	})
}