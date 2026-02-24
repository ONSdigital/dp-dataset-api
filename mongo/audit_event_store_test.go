package mongo

import (
	"context"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/models"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCreateAuditEvent(t *testing.T) {
	Convey("Given MongoDB is running", t, func() {
		ctx := context.Background()
		mongo, err := getTestMongoDB(ctx, t)
		So(err, ShouldBeNil)

		Convey("When CreateAuditEvent is called", func() {
			event, err := models.NewAuditEvent(
				models.RequestedBy{ID: "user-1", Email: "user1@example.com"},
				models.ActionCreate,
				"/datasets/dataset-1",
				&models.Dataset{ID: "dataset-1"},
				nil, nil, nil,
			)
			So(err, ShouldBeNil)

			err = mongo.CreateAuditEvent(ctx, event)

			Convey("Then the audit event is created successfully", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}
