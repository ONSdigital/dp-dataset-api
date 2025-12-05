package sdk

import (
	"errors"
	"net/http"
)

// ResponseHedaers represents headers that are available in the HTTP response
type ResponseHeaders struct {
	ETag string
}

var (
	// ErrHeaderNotFound returned if the requested header is not present in the provided request
	ErrHeaderNotFound = errors.New("header not found")

	// ErrValueEmpty returned if an empty value is passed when a non-empty value is required
	ErrValueEmpty = errors.New("header not set as value was empty")

	// ErrRequestNil return if SetX header function is called with a nil request
	ErrRequestNil = errors.New("error setting request header request was nil")

	// ErrResponseNil return if GetResponseX header function is called with a nil response
	ErrResponseNil = errors.New("error getting request header, response was nil")

	// eTagHeader is the ETag header name
	eTagHeader = "ETag"
)

// GetResponseETag returns the value of "ETag" response header if it exists, returns
// ErrResponseNil if the header is not found.
func GetResponseETag(resp *http.Response) (string, error) {
	return getResponseHeader(resp, eTagHeader)
}

func getResponseHeader(resp *http.Response, headerName string) (string, error) {
	if resp == nil {
		return "", ErrResponseNil
	}

	headerValue := resp.Header.Get(headerName)
	if headerValue == "" {
		return "", ErrHeaderNotFound
	}

	return headerValue, nil
}
