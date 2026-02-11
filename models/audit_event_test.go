package models

import (
	"errors"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNewAuditEvent(t *testing.T) {
	testCases := []struct {
		name        string
		requestedBy RequestedBy
		action      Action
		resource    string
		dataset     *Dataset
		version     *Version
		expectedErr error
	}{
		{
			name:        "both dataset and version are nil",
			requestedBy: RequestedBy{ID: "user-1"},
			action:      ActionCreate,
			resource:    "/datasets/dataset-1",
			dataset:     nil,
			version:     nil,
			expectedErr: errors.New("one of dataset, version, or metadata must be provided"),
		},
		{
			name:        "both dataset and version are provided",
			requestedBy: RequestedBy{ID: "user-1"},
			action:      ActionCreate,
			resource:    "/datasets/dataset-1",
			dataset:     &Dataset{ID: "dataset-1"},
			version:     &Version{ID: "version-1"},
			expectedErr: errors.New("one of dataset, version, or metadata must be provided"),
		},
		{
			name:        "only dataset is provided",
			requestedBy: RequestedBy{ID: "user-1"},
			action:      ActionCreate,
			resource:    "/datasets/dataset-1",
			dataset:     &Dataset{ID: "dataset-1"},
			version:     nil,
			expectedErr: nil,
		},
		{
			name:        "only version is provided",
			requestedBy: RequestedBy{ID: "user-1"},
			action:      ActionCreate,
			resource:    "/datasets/dataset-1/editions/2026/versions/1",
			dataset:     nil,
			version:     &Version{ID: "1"},
			expectedErr: nil,
		},
	}

	Convey("NewAuditEvent input validation", t, func() {
		for _, tc := range testCases {
			Convey(tc.name, func() {
				_, err := NewAuditEvent(tc.requestedBy, tc.action, tc.resource, tc.dataset, tc.version, nil)
				So(err, ShouldEqual, tc.expectedErr)
			})
		}
	})

	Convey("NewAuditEvent creates AuditEvent correctly", t, func() {
		requestedBy := RequestedBy{ID: "user-1", Email: "user1@example.com"}
		action := ActionUpdate
		resource := "/datasets/dataset-1"
		dataset := &Dataset{ID: "dataset-1"}

		auditEvent, err := NewAuditEvent(requestedBy, action, resource, dataset, nil, nil)
		So(err, ShouldBeNil)
		So(auditEvent.CreatedAt.IsZero(), ShouldBeFalse)
		So(auditEvent.RequestedBy, ShouldResemble, requestedBy)
		So(auditEvent.Action, ShouldEqual, action)
		So(auditEvent.Resource, ShouldEqual, resource)
		So(auditEvent.Dataset, ShouldResemble, dataset)
		So(auditEvent.Version, ShouldBeNil)
	})
}
