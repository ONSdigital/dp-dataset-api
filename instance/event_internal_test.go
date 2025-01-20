package instance

import (
	"strings"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestCreateEventWithBadReader(t *testing.T) {
	convey.Convey("Create an event with an invalid reader", t, func() {
		_, err := unmarshalEvent(Reader{})
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestCreateEventWithEmptyJson(t *testing.T) {
	convey.Convey("Create an event with empty json", t, func() {
		event, err := unmarshalEvent(strings.NewReader("{ }"))
		convey.So(err, convey.ShouldBeNil)
		convey.So(event.Validate(), convey.ShouldNotBeNil)
	})
}

func TestEventInstance(t *testing.T) {
	convey.Convey("Create an event with the required fields", t, func() {
		event, err := unmarshalEvent(strings.NewReader(`{"message": "321", "type": "error", "message_offset":"00", "time":"2017-08-25T15:09:11.829+01:00" }`))
		convey.So(err, convey.ShouldBeNil)
		convey.So(event.Validate(), convey.ShouldBeNil)
	})
}
