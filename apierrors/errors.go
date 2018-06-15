package apierrors

import "errors"

// A list of error messages for Dataset API
var (
	ErrAddDatasetAlreadyExists           = errors.New("forbidden - dataset already exists")
	ErrAddUpdateDatasetBadRequest        = errors.New("Failed to parse json body")
	ErrAuditActionAttemptedFailure       = errors.New("internal server error")
	ErrDatasetNotFound                   = errors.New("Dataset not found")
	ErrDeleteDatasetNotFound             = errors.New("dataset not found")
	ErrDeletePublishedDatasetForbidden   = errors.New("a published dataset cannot be deleted")
	ErrDimensionNodeNotFound             = errors.New("Dimension node not found")
	ErrDimensionNotFound                 = errors.New("Dimension not found")
	ErrDimensionOptionNotFound           = errors.New("Dimension option not found")
	ErrDimensionsNotFound                = errors.New("Dimensions not found")
	ErrEditionNotFound                   = errors.New("Edition not found")
	ErrIndexOutOfRange                   = errors.New("index out of range")
	ErrInstanceNotFound                  = errors.New("Instance not found")
	ErrInternalServer                    = errors.New("internal error")
	ErrMetadataVersionNotFound           = errors.New("Version not found")
	ErrMissingParameters                 = errors.New("missing properties in JSON")
	ErrMissingVersionHeadersOrDimensions = errors.New("missing headers or dimensions or both from version doc")
	ErrNoAuthHeader                      = errors.New("No authentication header provided")
	ErrObservationsNotFound              = errors.New("No observations found")
	ErrResourcePublished                 = errors.New("unable to update resource as it has been published")
	ErrResourceState                     = errors.New("Incorrect resource state")
	ErrTooManyWildcards                  = errors.New("only one wildcard (*) is allowed as a value in selected query parameters")
	ErrUnableToReadMessage               = errors.New("failed to read message body")
	ErrUnableToParseJSON                 = errors.New("failed to parse json body")
	ErrUnauthorised                      = errors.New("Unauthorised access to API")
	ErrVersionBadRequest                 = errors.New("Failed to parse json body")
	ErrVersionMissingState               = errors.New("Missing state from version")
	ErrVersionNotFound                   = errors.New("Version not found")

	NotFoundMap = map[error]bool{
		ErrDatasetNotFound:         true,
		ErrDimensionNodeNotFound:   true,
		ErrDimensionOptionNotFound: true,
		ErrEditionNotFound:         true,
		ErrInstanceNotFound:        true,
		ErrVersionNotFound:         true,

		ErrDimensionNodeNotFound: true,
		ErrDimensionNotFound:     true,
	}

	BadRequestMap = map[error]bool{
		ErrMissingParameters:   true,
		ErrUnableToParseJSON:   true,
		ErrUnableToReadMessage: true,
	}
)
