package apierrors

import "errors"

// NotFound error messages for Dataset API
var (
	ErrDatasetNotFound       = errors.New("Dataset not found")
	ErrEditionNotFound       = errors.New("Edition not found")
	ErrVersionNotFound       = errors.New("Version not found")
	ErrDimensionNodeNotFound = errors.New("Dimension node not found")
	ErrDimensionNotFound     = errors.New("Dimension not found")
	ErrDimensionsNotFound    = errors.New("Dimensions not found")
	ErrInstanceNotFound      = errors.New("Instance not found")
)
