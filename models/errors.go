package models

import (
	"context"
	"errors"

	"github.com/ONSdigital/log.go/v2/log"
)

// Error represents a custom error type with additional context and description.
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

// NewError creates and logs a new Error with the provided context, cause, code, and description.
func NewError(ctx context.Context, cause error, code, description string) Error {
	err := Error{
		Cause:       cause,
		Code:        code,
		Description: description,
	}
	log.Error(ctx, description, err)
	return err
}

// NewValidationError creates a new Error specifically for validation errors with a code and description.
func NewValidationError(ctx context.Context, code, description string) Error {
	err := Error{
		Cause:       errors.New(code),
		Code:        code,
		Description: description,
	}
	log.Error(ctx, description, err, log.Data{"code": code})
	return err
}
