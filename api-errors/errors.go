package api_errors

import "errors"

var DatasetNotFound = errors.New("Dataset not found")

var EditionNotFound = errors.New("Edition not found")

var VersionNotFound = errors.New("Version not found")