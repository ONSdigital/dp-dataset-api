package dimension

import (
	"strings"
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	. "github.com/smartystreets/goconvey/convey"
)

func TestUnmarshalDimensionCache(t *testing.T) {
	t.Parallel()
	Convey("Successfully unmarshal dimension cache", t, func() {
		json := strings.NewReader(`{"option":"24", "code_list":"123-456", "dimension": "test"}`)

		option, err := unmarshalDimensionCache(json)
		So(err, ShouldBeNil)
		So(option.CodeList, ShouldEqual, "123-456")
		So(option.Name, ShouldEqual, "test")
		So(option.Option, ShouldEqual, "24")
	})

	Convey("Fail to unmarshal dimension cache", t, func() {
		Convey("When unable to marshal json", func() {
			json := strings.NewReader("{")

			option, err := unmarshalDimensionCache(json)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errs.ErrUnableToParseJSON)
			So(option, ShouldBeNil)
		})

		Convey("When options are missing mandatory fields", func() {
			json := strings.NewReader("{}")

			option, err := unmarshalDimensionCache(json)
			So(err, ShouldNotBeNil)
			So(err, ShouldResemble, errs.ErrMissingParameters)
			So(option, ShouldBeNil)
		})
	})
}
