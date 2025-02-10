package instance

import (
	"context"
	"fmt"
	"strings"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	. "github.com/smartystreets/goconvey/convey"
)

// Reader used to trigger errors on reading
type Reader struct {
}

// Read a function used to mock errors
func (f Reader) Read(_ []byte) (int, error) {
	return 0, fmt.Errorf("Reader failed")
}

var ctx = context.Background()

func TestUnmarshalInstanceWithBadReader(t *testing.T) {
	Convey("Create an instance with an invalid reader", t, func() {
		instance, err := UnmarshalInstance(ctx, Reader{}, true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, "failed to read message body")
	})
}

func TestUnmarshalInstanceWithInvalidJson(t *testing.T) {
	Convey("Create an instance with invalid json", t, func() {
		instance, err := UnmarshalInstance(ctx, strings.NewReader("{ "), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
	})
}

func TestUnmarshalInstanceWithEmptyJson(t *testing.T) {
	Convey("Create an instance with empty json", t, func() {
		instance, err := UnmarshalInstance(ctx, strings.NewReader("{ }"), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, errs.ErrMissingJobProperties.Error())
	})

	Convey("Create an instance with empty job link", t, func() {
		instance, err := UnmarshalInstance(ctx, strings.NewReader(`{"links":{"job": null}}`), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, errs.ErrMissingJobProperties.Error())
	})

	Convey("Create an instance with empty href in job link", t, func() {
		instance, err := UnmarshalInstance(ctx, strings.NewReader(`{"links":{"job":{"id": "456"}}}`), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, errs.ErrMissingJobProperties.Error())
	})

	Convey("Create an instance with empty href in job link", t, func() {
		instance, err := UnmarshalInstance(ctx, strings.NewReader(`{"links":{"job":{"href": "http://localhost:21800/jobs/456"}}}`), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, errs.ErrMissingJobProperties.Error())
	})

	Convey("Update an instance with empty json", t, func() {
		instance, err := UnmarshalInstance(ctx, strings.NewReader("{ }"), false)
		So(instance, ShouldNotBeEmpty)
		So(err, ShouldBeNil)
	})
}

func TestUnmarshalInstanceWithMissingFields(t *testing.T) {
	Convey("Create an instance with no id", t, func() {
		instance, err := UnmarshalInstance(ctx, strings.NewReader(`{"links": { "job": { "link":"http://localhost:2200/jobs/123-456" } }}`), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, errs.ErrMissingJobProperties.Error())
	})

	Convey("Create an instance with no link", t, func() {
		instance, err := UnmarshalInstance(ctx, strings.NewReader(`{"links": { "job": {"id":"123-456"} }}`), true)
		So(instance, ShouldBeNil)
		So(err.Error(), ShouldEqual, errs.ErrMissingJobProperties.Error())
	})

	Convey("Update an instance with no id", t, func() {
		instance, err := UnmarshalInstance(ctx, strings.NewReader(`{"links": { "job": { "link":"http://localhost:2200/jobs/123-456" } }}`), false)
		So(instance, ShouldNotBeNil)
		So(err, ShouldBeNil)
	})

	Convey("Update an instance with no link", t, func() {
		instance, err := UnmarshalInstance(ctx, strings.NewReader(`{"links": { "job": {"id":"123-456"} }}`), false)
		So(instance, ShouldNotBeNil)
		So(err, ShouldBeNil)
	})
}

func TestUnmarshalInstance(t *testing.T) {
	Convey("Create an instance with the required fields", t, func() {
		instance, err := UnmarshalInstance(ctx, strings.NewReader(`{"links": { "job": { "id":"123-456", "href":"http://localhost:2200/jobs/123-456" } }}`), true)
		So(err, ShouldBeNil)
		So(instance.Links.Job.ID, ShouldEqual, "123-456")
	})
}
