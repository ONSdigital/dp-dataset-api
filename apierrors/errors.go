package apierrors

import "errors"

// Error messages for Dataset API
var (
	ErrDatasetNotFound                   = errors.New("Dataset not found")
	ErrAddDatasetAlreadyExists           = errors.New("forbidden - dataset already exists")
	ErrAddDatasetBadRequest              = errors.New("Failed to parse json body")
	ErrEditionNotFound                   = errors.New("Edition not found")
	ErrVersionNotFound                   = errors.New("Version not found")
	ErrDimensionNodeNotFound             = errors.New("Dimension node not found")
	ErrDimensionNotFound                 = errors.New("Dimension not found")
	ErrDimensionsNotFound                = errors.New("Dimensions not found")
	ErrInstanceNotFound                  = errors.New("Instance not found")
	ErrUnauthorised                      = errors.New("Unauthorised access to API")
	ErrNoAuthHeader                      = errors.New("No authentication header provided")
	ErrResourceState                     = errors.New("Incorrect resource state")
	ErrVersionMissingState               = errors.New("Missing state from version")
	ErrInternalServer                    = errors.New("internal error")
	ErrObservationsNotFound              = errors.New("No observations found")
	ErrIndexOutOfRange                   = errors.New("index out of range")
	ErrMissingVersionHeadersOrDimensions = errors.New("missing headers or dimensions or both from version doc")
	ErrTooManyWildcards                  = errors.New("only one wildcard (*) is allowed as a value in selected query parameters")
	ErrDeletePublishedDatasetForbidden   = errors.New("a published dataset cannot be deleted")
	ErrDeleteDatasetNotFound             = errors.New("dataset not found")
	ErrAuditActionAttemptedFailure       = errors.New("internal server error")

	// metadata endpoint errors
	ErrMetadataVersionNotFound = errors.New("Version not found")
)
