package apierrors

import "errors"

// NotFound error messages for Dataset API
var (
	DatasetNotFound       = errors.New("Dataset not found")
	EditionNotFound       = errors.New("Edition not found")
	VersionNotFound       = errors.New("Version not found")
	DimensionNodeNotFound = errors.New("Dimension node not found")
	InstanceNotFound      = errors.New("Instance not found")
)
