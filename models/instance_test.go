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
		instance, err := CreateInstance(strings.NewReader(`{"links":{ "job": { "id":"123-456", "href":"http://localhost:2200/jobs/123-456" } } }`))
		So(err, ShouldBeNil)
		So(instance.Defaults(), ShouldBeNil)
		So(instance.Links.Job.ID, ShouldEqual, "123-456")
	})
}

func TestCreateEventWithBadReader(t *testing.T) {
	Convey("Create an event with an invalid reader", t, func() {
		_, err := CreateEvent(Reader{})
		So(err, ShouldNotBeNil)
	})
}

func TestCreateEventWithEmptyJson(t *testing.T) {
	Convey("Create an event with empty json", t, func() {
		event, err := CreateEvent(strings.NewReader("{ }"))
		So(err, ShouldBeNil)
		So(event.Validate(), ShouldNotBeNil)
	})
}

func TestEventInstance(t *testing.T) {
	Convey("Create an event with the required fields", t, func() {
		event, err := CreateEvent(strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`))
		So(err, ShouldBeNil)
		So(event.Validate(), ShouldBeNil)
	})
}