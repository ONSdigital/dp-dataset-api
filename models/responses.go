package models

type ErrorResponse struct {
	Errors  []error           `json:"errors"`
	Status  int               `json:"-"`
	Headers map[string]string `json:"-"`
}

func NewErrorResponse(statusCode int, headers map[string]string, errors ...error) *ErrorResponse {
	return &ErrorResponse{
		Errors:  errors,
		Status:  statusCode,
		Headers: headers,
	}
}

type SuccessResponse struct {
	Body    []byte            `json:"-"`
	Status  int               `json:"-"`
	Headers map[string]string `json:"-"`
}

func NewSuccessResponse(jsonBody []byte, statusCode int, headers map[string]string) *SuccessResponse {
	return &SuccessResponse{
		Body:    jsonBody,
		Status:  statusCode,
		Headers: headers,
	}
}
