package application

import (
	"context"
	"errors"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/ONSdigital/dp-dataset-api/store"
	storetest "github.com/ONSdigital/dp-dataset-api/store/datastoretest"
	. "github.com/smartystreets/goconvey/convey"
)

// This test covers RecordDatasetAuditEvent, RecordVersionAuditEvent, and indirectly recordAuditEvent
func TestAuditService_RecordAuditEvent(t *testing.T) {
	Convey("Given a mocked DataStore", t, func() {
		mockDataStore := &storetest.StorerMock{
			CreateAuditEventFunc: func(ctx context.Context, event *models.AuditEvent) error {
				return nil
			},
		}

		auditService := NewAuditService(store.DataStore{Backend: mockDataStore})

		Convey("When RecordDatasetAuditEvent is called successfully", func() {
			err := auditService.RecordDatasetAuditEvent(context.Background(),
				models.RequestedBy{ID: "user-1", Email: "user1@example.com"},
				models.ActionCreate,
				"/datasets/dataset-1",
				&models.Dataset{ID: "dataset-1"},
			)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When RecordVersionAuditEvent is called successfully", func() {
			err := auditService.RecordVersionAuditEvent(context.Background(),
				models.RequestedBy{ID: "user-1", Email: "user1@example.com"},
				models.ActionCreate,
				"/datasets/dataset-1/editions/2026/versions/1",
				&models.Version{ID: "version-1"},
			)

			Convey("Then no error is returned", func() {
				So(err, ShouldBeNil)
			})
		})

		Convey("When RecordVersionAuditEvent is called with a nil version", func() {
			err := auditService.RecordVersionAuditEvent(context.Background(),
				models.RequestedBy{ID: "user-1", Email: "user1@example.com"},
				models.ActionCreate,
				"/datasets/dataset-1/editions/2026/versions/1",
				nil,
			)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "recordAuditEvent: failed to create audit event model: one of dataset, version, or metadata must be provided")
			})
		})

		Convey("When RecordVersionAuditEvent is called and the DataStore returns an error", func() {
			mockDataStore.CreateAuditEventFunc = func(ctx context.Context, event *models.AuditEvent) error {
				return errors.New("datastore error")
			}

			err := auditService.RecordVersionAuditEvent(context.Background(),
				models.RequestedBy{ID: "user-1", Email: "user1@example.com"},
				models.ActionCreate,
				"/datasets/dataset-1/editions/2026/versions/1",
				&models.Version{ID: "version-1"},
			)

			Convey("Then an error is returned", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "recordAuditEvent: failed to create audit event in store: datastore error")
			})
		})
	})
}
