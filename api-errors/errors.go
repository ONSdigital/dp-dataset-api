package api_errors

import "errors"

var DatasetNotFound = errors.New("Dataset not found")

var EditionNotFound = errors.New("Edition not found")

var VersionNotFound = errors.New("Version not found")

var DimensionNodeNotFound = errors.New("Dimension node not found")

var InstanceNotFound = errors.New("Instance not found")