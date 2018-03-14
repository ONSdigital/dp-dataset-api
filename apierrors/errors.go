package apierrors

import "errors"

// Error messages for Dataset API
var (
	ErrDatasetNotFound       = errors.New("Dataset not found")
	ErrEditionNotFound       = errors.New("Edition not found")
	ErrVersionNotFound       = errors.New("Version not found")
	ErrDimensionNodeNotFound = errors.New("Dimension node not found")
	ErrDimensionNotFound     = errors.New("Dimension not found")
	ErrDimensionsNotFound    = errors.New("Dimensions not found")
	ErrInstanceNotFound      = errors.New("Instance not found")
	ErrUnauthorised          = errors.New("Unauthorised access to API")
	ErrNoAuthHeader          = errors.New("No authentication header provided")
	ErrResourceState         = errors.New("Incorrect resource state")
	ErrVersionMissingState   = errors.New("Missing state from version")
	ErrInternalServer        = errors.New("internal error")
)
