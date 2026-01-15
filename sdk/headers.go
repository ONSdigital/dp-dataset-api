package sdk

import (
	"errors"
	"net/http"
	"strings"

	dpNetRequest "github.com/ONSdigital/dp-net/v3/request"
)

// ResponseHeaders represents headers that are available in the HTTP response
type ResponseHeaders struct {
	ETag string
}

const (
	ifMatchHeader = "If-Match"
	eTagHeader    = "ETag"
)

var (
	// ErrHeaderNotFound returned if the requested header is not present in the provided request
	ErrHeaderNotFound = errors.New("header not found")

	// ErrResponseNil return if GetResponseX header function is called with a nil response
	ErrResponseNil = errors.New("error getting request header, response was nil")
)

// Contains the headers to be added to any request
type Headers struct {
	CollectionID         string
	DownloadServiceToken string
	AccessToken          string // could be user or service token for auth v2
	IfMatch              string
}

// Adds headers to the input request
func (h *Headers) add(request *http.Request) {
	if h.CollectionID != "" {
		request.Header.Add(dpNetRequest.CollectionIDHeaderKey, h.CollectionID)
	}

	dpNetRequest.AddDownloadServiceTokenHeader(request, h.DownloadServiceToken)

	// Adding the service token header appends the Bearer prefix to the value submitted
	// If it's present this needs to be removed as otherwise the token provided is not valid
	if strings.Contains(h.AccessToken, "Bearer ") {
		h.AccessToken = strings.ReplaceAll(h.AccessToken, "Bearer ", "")
	}

	dpNetRequest.AddServiceTokenHeader(request, h.AccessToken)

	if h.IfMatch != "" {
		request.Header.Add(ifMatchHeader, h.IfMatch)
	}
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
