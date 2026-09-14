package utils

import (
	"regexp"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
)

// Regular expression to validate ID format (letters, numbers, and dashes only)
var validIDFormat = regexp.MustCompile(`^[A-Za-z0-9-]*$`)

// ValidateIDFormat checks if the provided ID contains only letters, numbers and dashes.
func ValidateIDFormat(id string) error {
	if !validIDFormat.MatchString(id) {
		return errs.ErrInvalidID
	}
	return nil
}
