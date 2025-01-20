package instance

import (
	"context"
	"fmt"
	"strings"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/smartystreets/goconvey/convey"
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
	convey.Convey("Create an instance with an invalid reader", t, func() {
		instance, err := unmarshalInstance(ctx, Reader{}, true)
		convey.So(instance, convey.ShouldBeNil)
		convey.So(err.Error(), convey.ShouldEqual, "failed to read message body")
	})
}

func TestUnmarshalInstanceWithInvalidJson(t *testing.T) {
	convey.Convey("Create an instance with invalid json", t, func() {
		instance, err := unmarshalInstance(ctx, strings.NewReader("{ "), true)
		convey.So(instance, convey.ShouldBeNil)
		convey.So(err.Error(), convey.ShouldContainSubstring, errs.ErrUnableToParseJSON.Error())
	})
}

func TestUnmarshalInstanceWithEmptyJson(t *testing.T) {
	convey.Convey("Create an instance with empty json", t, func() {
		instance, err := unmarshalInstance(ctx, strings.NewReader("{ }"), true)
		convey.So(instance, convey.ShouldBeNil)
		convey.So(err.Error(), convey.ShouldEqual, errs.ErrMissingJobProperties.Error())
	})

	convey.Convey("Create an instance with empty job link", t, func() {
		instance, err := unmarshalInstance(ctx, strings.NewReader(`{"links":{"job": null}}`), true)
		convey.So(instance, convey.ShouldBeNil)
		convey.So(err.Error(), convey.ShouldEqual, errs.ErrMissingJobProperties.Error())
	})

	convey.Convey("Create an instance with empty href in job link", t, func() {
		instance, err := unmarshalInstance(ctx, strings.NewReader(`{"links":{"job":{"id": "456"}}}`), true)
		convey.So(instance, convey.ShouldBeNil)
		convey.So(err.Error(), convey.ShouldEqual, errs.ErrMissingJobProperties.Error())
	})

	convey.Convey("Create an instance with empty href in job link", t, func() {
		instance, err := unmarshalInstance(ctx, strings.NewReader(`{"links":{"job":{"href": "http://localhost:21800/jobs/456"}}}`), true)
		convey.So(instance, convey.ShouldBeNil)
		convey.So(err.Error(), convey.ShouldEqual, errs.ErrMissingJobProperties.Error())
	})

	convey.Convey("Update an instance with empty json", t, func() {
		instance, err := unmarshalInstance(ctx, strings.NewReader("{ }"), false)
		convey.So(instance, convey.ShouldNotBeEmpty)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestUnmarshalInstanceWithMissingFields(t *testing.T) {
	convey.Convey("Create an instance with no id", t, func() {
		instance, err := unmarshalInstance(ctx, strings.NewReader(`{"links": { "job": { "link":"http://localhost:2200/jobs/123-456" } }}`), true)
		convey.So(instance, convey.ShouldBeNil)
		convey.So(err.Error(), convey.ShouldEqual, errs.ErrMissingJobProperties.Error())
	})

	convey.Convey("Create an instance with no link", t, func() {
		instance, err := unmarshalInstance(ctx, strings.NewReader(`{"links": { "job": {"id":"123-456"} }}`), true)
		convey.So(instance, convey.ShouldBeNil)
		convey.So(err.Error(), convey.ShouldEqual, errs.ErrMissingJobProperties.Error())
	})

	convey.Convey("Update an instance with no id", t, func() {
		instance, err := unmarshalInstance(ctx, strings.NewReader(`{"links": { "job": { "link":"http://localhost:2200/jobs/123-456" } }}`), false)
		convey.So(instance, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("Update an instance with no link", t, func() {
		instance, err := unmarshalInstance(ctx, strings.NewReader(`{"links": { "job": {"id":"123-456"} }}`), false)
		convey.So(instance, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestUnmarshalInstance(t *testing.T) {
	convey.Convey("Create an instance with the required fields", t, func() {
		instance, err := unmarshalInstance(ctx, strings.NewReader(`{"links": { "job": { "id":"123-456", "href":"http://localhost:2200/jobs/123-456" } }}`), true)
		convey.So(err, convey.ShouldBeNil)
		convey.So(instance.Links.Job.ID, convey.ShouldEqual, "123-456")
	})
}
