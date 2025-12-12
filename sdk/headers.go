package sdk

import (
	"errors"
	"net/http"

	dpNetRequest "github.com/ONSdigital/dp-net/v3/request"
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

// Contains the headers to be added to any request
type Headers struct {
	CollectionID         string
	DownloadServiceToken string
	AccessToken          string // could be user or service token for auth v2
}

// Adds headers to the input request
func (h *Headers) add(request *http.Request) {
	if h.CollectionID != "" {
		request.Header.Add(dpNetRequest.CollectionIDHeaderKey, h.CollectionID)
	}
	dpNetRequest.AddDownloadServiceTokenHeader(request, h.DownloadServiceToken)
	dpNetRequest.AddServiceTokenHeader(request, h.AccessToken)
}

// SetIfMatch set the If-Match header on the provided request. If this header is already present it
// will be overwritten by the new value. Empty values are allowed for this header.
func setIfMatch(req *http.Request, headerValue string) error {
	err := setRequestHeader(req, ifMatchHeader, headerValue)
	if err != nil && err != ErrValueEmpty {
		return err
	}
	return nil
}

func setRequestHeader(req *http.Request, headerName, headerValue string) error {
	if req == nil {
		return ErrRequestNil
	}

	if headerValue == "" {
		return ErrValueEmpty
	}

	req.Header.Set(headerName, headerValue)
	return nil
}

// GetResponseETag returns the value of "ETag" response header if it exists, returns
// ErrResponseNil if the header is not found.
func getResponseETag(resp *http.Response) (string, error) {
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
