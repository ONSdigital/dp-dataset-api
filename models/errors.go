package models

import (
	"errors"
)

// Error represents a custom error type containing a cause, code, and description.
type Error struct {
	Cause       error  `json:"-"`           // The underlying error, if available.
	Code        string `json:"code"`        // Error code representing the type of error.
	Description string `json:"description"` // Detailed description of the error.
}

// Error returns the error message string for the custom Error type.
func (e Error) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return e.Code + ": " + e.Description
}

// NewError creates a new Error with the given cause, code, and description.
func NewError(cause error, code, description string) Error {
	err := Error{
		Cause:       cause,
		Code:        code,
		Description: description,
	}
	return err
}

// NewValidationError creates a new Error specifically for validation errors with a code and description.
func NewValidationError(code, description string) Error {
	err := Error{
		Cause:       errors.New(code),
		Code:        code,
		Description: description,
	}
	return err
}
