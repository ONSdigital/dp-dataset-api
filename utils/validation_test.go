package utils

import (
	"testing"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	. "github.com/smartystreets/goconvey/convey"
)

func TestValidateIDFormat(t *testing.T) {
	testCases := []struct {
		name        string
		id          string
		expectedErr error
	}{
		{
			name:        "accepts lowercase letters, numbers and dashes",
			id:          "valid-dataset-id-123",
			expectedErr: nil,
		},
		{
			name:        "accepts uppercase letters",
			id:          "Valid-Dataset-ID-123",
			expectedErr: nil,
		},
		{
			name:        "accepts an empty ID",
			id:          "",
			expectedErr: nil,
		},
		{
			name:        "rejects spaces",
			id:          "invalid dataset id",
			expectedErr: errs.ErrInvalidID,
		},
		{
			name:        "rejects underscores",
			id:          "invalid_dataset_id",
			expectedErr: errs.ErrInvalidID,
		},
		{
			name:        "rejects special characters",
			id:          "invalid-dataset-id!",
			expectedErr: errs.ErrInvalidID,
		},
	}

	for _, tc := range testCases {
		Convey(tc.name, t, func() {
			err := ValidateIDFormat(tc.id)
			So(err, ShouldEqual, tc.expectedErr)
		})
	}
}
