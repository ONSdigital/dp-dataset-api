package models

const (
	BodyReadError            = "RequestBodyReadError"
	JSONMarshalError         = "JSONMarshalError"
	JSONUnmarshalError       = "JSONUnmarshalError"
	WriteResponseError       = "WriteResponseError"
	ErrEditionNotFound       = "ErrEditionNotFound"
	ErrDatasetNotFound       = "ErrDatasetNotFound"
	ErrInvalidQueryParameter = "ErrInvalidQueryParameter"
	ErrMissingParameters     = "ErrMissingParameters"
	ErrVersionAlreadyExists  = "ErrVersionAlreadyExists"
	InternalError            = "InternalServerError"
	NotFoundError            = "NotFound"
	MissingConfigError       = "MissingConfig"
	UnknownRequestTypeError  = "UnknownRequestType"
	NotImplementedError      = "NotImplemented"
	BodyCloseError           = "BodyCloseError"
	ErrInvalidTypeError      = "InvalidType"
)

// API error descriptions
const (
	ErrorMarshalFailedDescription                 = "failed to marshal the error"
	ErrorUnmarshalFailedDescription               = "failed to unmarshal the request body"
	WriteResponseFailedDescription                = "failed to write http response"
	BodyReadFailedDescription                     = "endpoint returned an error reading the request body"
	MissingConfigDescription                      = "required configuration setting is missing"
	NotImplementedDescription                     = "this feature has not been implemented yet"
	BodyClosedFailedDescription                   = "the request body failed to close"
	InternalErrorDescription                      = "Internal Server Error"
	ErrDatasetNotFoundDescription                 = "dataset not found"
	ErrEditionNotFoundDescription                 = "edition not found"
	ErrInvalidQueryParameterDescription           = "invalid query parameter"
	ErrMissingParametersDescription               = "missing properties in JSON"
	ErrUnpublishedVersionAlreadyExistsDescription = "an unpublished version of this dataset already exists"
	ErrVersionAlreadyExistsDescription            = "version already exists"
	ErrTypeNotStaticDescription                   = "version type should be static"
)
