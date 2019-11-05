package auth

import (
	"fmt"
	"net/http"

	"github.com/ONSdigital/dp-api-clients-go/headers"
)

// PermissionsRequestBuilder is an implementation of the GetPermissionsRequestBuilder interface that creates a
// user permissions http request from an inbound http request.
// Host - the host of Permissions API.
type PermissionsRequestBuilder struct {
	Host string
}

// NewPermissionsRequestBuilder is a constructor method for creating a new PermissionsRequestBuilder
// Host - the host of Permissions API.
func NewPermissionsRequestBuilder(host string) GetPermissionsRequestBuilder {
	return &PermissionsRequestBuilder{Host: host}
}

// NewPermissionsRequest create a new get permissions http request from the inbound request.
func (builder *PermissionsRequestBuilder) NewPermissionsRequest(req *http.Request) (*http.Request, error) {
	if err := builder.checkConfiguration(); err != nil {
		return nil, err
	}

	if req == nil {
		return nil, requestRequiredButNilError
	}

	userAuthToken, serviceAuthToken, err := getAuthTokens(req)
	if err != nil {
		return nil, err
	}

	if userAuthToken != "" {
		return builder.createUserPermissionsRequest(userAuthToken)
	}

	return builder.createServicePermissionsRequest(serviceAuthToken)
}

func (builder *PermissionsRequestBuilder) createUserPermissionsRequest(authToken string) (*http.Request, error) {
	url := fmt.Sprintf(userInstancePermissionsURL, builder.Host)
	getPermissionsRequest, err := createRequest(url)
	if err != nil {
		return nil, err
	}

	if err := headers.SetUserAuthToken(getPermissionsRequest, authToken); err != nil {
		return nil, err
	}

	return getPermissionsRequest, nil
}

func (builder *PermissionsRequestBuilder) createServicePermissionsRequest(serviceAuthToken string) (*http.Request, error) {
	url := fmt.Sprintf(serviceInstancePermissionsURL, builder.Host)
	getPermissionsRequest, err := createRequest(url)
	if err != nil {
		return nil, err
	}

	if err := headers.SetServiceAuthToken(getPermissionsRequest, serviceAuthToken); err != nil {
		return nil, err
	}

	return getPermissionsRequest, nil
}

func (builder *PermissionsRequestBuilder) checkConfiguration() error {
	if builder.Host == "" {
		return Error{
			Status:  500,
			Message: "PermissionsRequestBuilder configuration invalid host required but was empty",
		}
	}
	return nil
}

// getAuthTokens get the user and or service auth tokens from the request. 
func getAuthTokens(req *http.Request) (string, string, error) {
	userAuthToken, errUserToken := headers.GetUserAuthToken(req)
	if errUserToken != nil && headers.IsNotErrNotFound(errUserToken) {
		// something has gone wrong - bail
		return "", "", errUserToken
	}

	serviceAuthToken, errServiceToken := headers.GetServiceAuthToken(req)
	if errServiceToken != nil && headers.IsNotErrNotFound(errServiceToken) {
		// something has gone wrong - bail
		return "", "", errServiceToken
	}

	if headers.IsErrNotFound(errUserToken) && headers.IsErrNotFound(errServiceToken) {
		// neither token found - bail with error.
		return "", "", noUserOrServiceAuthTokenProvidedError
	}
	return userAuthToken, serviceAuthToken, nil
}
