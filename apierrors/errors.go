package apierrors

import "errors"

// A list of error messages for Dataset API
var (
	ErrAddDatasetAlreadyExists           = errors.New("forbidden - dataset already exists")
	ErrAddUpdateDatasetBadRequest        = errors.New("failed to parse json body")
	ErrAuditActionAttemptedFailure       = errors.New("internal server error")
	ErrDatasetNotFound                   = errors.New("dataset not found")
	ErrDeleteDatasetNotFound             = errors.New("dataset not found")
	ErrDeletePublishedDatasetForbidden   = errors.New("a published dataset cannot be deleted")
	ErrDimensionNodeNotFound             = errors.New("dimension node not found")
	ErrDimensionNotFound                 = errors.New("dimension not found")
	ErrDimensionOptionNotFound           = errors.New("dimension option not found")
	ErrDimensionsNotFound                = errors.New("dimensions not found")
	ErrEditionNotFound                   = errors.New("edition not found")
	ErrIndexOutOfRange                   = errors.New("index out of range")
	ErrInstanceNotFound                  = errors.New("instance not found")
	ErrInternalServer                    = errors.New("internal error")
	ErrInsertedObservationsInvalidSyntax = errors.New("inserted observation request parameter not a integer")
	ErrMetadataVersionNotFound           = errors.New("version not found")
	ErrMissingJobProperties              = errors.New("missing job properties")
	ErrMissingParameters                 = errors.New("missing properties in JSON")
	ErrMissingVersionHeadersOrDimensions = errors.New("missing headers or dimensions or both from version doc")
	ErrNoAuthHeader                      = errors.New("no authentication header provided")
	ErrObservationsNotFound              = errors.New("no observations found")
	ErrResourcePublished                 = errors.New("unable to update resource as it has been published")
	ErrResourceState                     = errors.New("incorrect resource state")
	ErrTooManyWildcards                  = errors.New("only one wildcard (*) is allowed as a value in selected query parameters")
	ErrUnableToParseJSON                 = errors.New("failed to parse json body")
	ErrUnableToReadMessage               = errors.New("failed to read message body")
	ErrUnauthorised                      = errors.New("unauthorised access to API")
	ErrVersionMissingState               = errors.New("missing state from version")
	ErrVersionNotFound                   = errors.New("version not found")

	NotFoundMap = map[error]bool{
		ErrDatasetNotFound:         true,
		ErrDimensionNotFound:       true,
		ErrDimensionsNotFound:      true,
		ErrDimensionNodeNotFound:   true,
		ErrDimensionOptionNotFound: true,
		ErrEditionNotFound:         true,
		ErrInstanceNotFound:        true,
		ErrVersionNotFound:         true,
	}

	BadRequestMap = map[error]bool{
		ErrInsertedObservationsInvalidSyntax: true,
		ErrMissingJobProperties:              true,
		ErrMissingParameters:                 true,
		ErrUnableToParseJSON:                 true,
		ErrUnableToReadMessage:               true,
	}
)
