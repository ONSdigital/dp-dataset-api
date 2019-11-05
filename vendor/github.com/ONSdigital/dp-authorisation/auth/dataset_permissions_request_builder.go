package auth

import (
	"fmt"
	"net/http"

	"github.com/ONSdigital/dp-api-clients-go/headers"
)

// DatasetPermissionsRequestBuilder is an implementation of the GetPermissionsRequestBuilder interface that creates a
// user datasets permissions request from an inbound http request.
// Host - the host of Permisssions API.
// DatasetIDKey - the placeholder name of the dataset ID URL variable.
// GetRequestVarsFunc - a function for getting request variables.
type DatasetPermissionsRequestBuilder struct {
	Host               string
	DatasetIDKey       string
	GetRequestVarsFunc func(r *http.Request) map[string]string
}

type parameters struct {
	userAuthToken    string
	serviceAuthToken string
	collectionID     string
	datasetID        string
}

type GetRequestVarsFunc func(r *http.Request) map[string]string

// NewDatasetPermissionsRequestBuilder is a constructor function for creating a new DatasetPermissionsRequestBuilder.
// Host - the host of Permisssions API.
// DatasetIDKey - the placeholder name of the dataset ID URL variable.
// GetRequestVarsFunc - a function for getting request variables.
func NewDatasetPermissionsRequestBuilder(host string, datasetIDKey string, getRequestVarsFunc GetRequestVarsFunc) GetPermissionsRequestBuilder {
	return &DatasetPermissionsRequestBuilder{
		Host:               host,
		DatasetIDKey:       datasetIDKey,
		GetRequestVarsFunc: getRequestVarsFunc,
	}
}

// NewPermissionsRequest fulfilling the GetPermissionsRequestBuilder interface. Create a new  get user/service account
// dataset permissions http requests. The req parameter is the inbound http.Request to generate the get permissions
// request from.
func (builder *DatasetPermissionsRequestBuilder) NewPermissionsRequest(req *http.Request) (*http.Request, error) {
	if err := builder.checkConfiguration(); err != nil {
		return nil, err
	}

	if req == nil {
		return nil, requestRequiredButNilError
	}

	parameters := builder.extractRequestParameters(req)
	if err := parameters.isValid(); err != nil {
		return nil, err
	}

	if parameters.isUserRequest() {
		return builder.createUserDatasetPermissionsRequest(parameters)
	}

	return builder.createServiceDatasetPermissionsRequest(parameters)
}

// extractRequestParameters helper function get the required headers and parameters from the inbound request.
func (builder *DatasetPermissionsRequestBuilder) extractRequestParameters(req *http.Request) parameters {
	// ignore errors and continue with empty string.
	userAuthToken, _ := headers.GetUserAuthToken(req)
	serviceAuthToken, _ := headers.GetServiceAuthToken(req)
	collectionID, _ := headers.GetCollectionID(req)

	return parameters{
		userAuthToken:    userAuthToken,
		serviceAuthToken: serviceAuthToken,
		collectionID:     collectionID,
		datasetID:        builder.GetRequestVarsFunc(req)[builder.DatasetIDKey],
	}
}

// createUserDatasetPermissionsRequest creates a new get user dataset permissions http.Request from the parameters provided.
func (builder *DatasetPermissionsRequestBuilder) createUserDatasetPermissionsRequest(params parameters) (*http.Request, error) {
	url := fmt.Sprintf(userDatasetPermissionsURL, builder.Host, params.datasetID, params.collectionID)

	getPermissionsReq, err := createRequest(url)
	if err != nil {
		return nil, err
	}

	if err := headers.SetUserAuthToken(getPermissionsReq, params.userAuthToken); err != nil {
		return nil, err
	}

	return getPermissionsReq, nil
}

// DatasetPermissionsRequestBuilder creates a new get service dataset permissions http.Request from the parameters provided.
func (builder *DatasetPermissionsRequestBuilder) createServiceDatasetPermissionsRequest(params parameters) (*http.Request, error) {
	url := fmt.Sprintf(serviceDatasetPermissionsURL, builder.Host, params.datasetID)

	getPermissionsReq, err := createRequest(url)
	if err != nil {
		return nil, err
	}

	if err = headers.SetServiceAuthToken(getPermissionsReq, params.serviceAuthToken); err != nil {
		return nil, err
	}

	return getPermissionsReq, nil
}

// checkConfiguration is a verify function that checks the required build is configured correctly. Returns an appropriate
// error if config is invalid or missing.
func (builder *DatasetPermissionsRequestBuilder) checkConfiguration() error {
	if builder.Host == "" {
		return Error{
			Status:  500,
			Message: "DatasetPermissionsRequestBuilder configuration invalid host required but was empty",
		}
	}
	if builder.DatasetIDKey == "" {
		return Error{
			Status:  500,
			Message: "DatasetPermissionsRequestBuilder configuration invalid datasetID key required but was empty",
		}
	}
	if builder.GetRequestVarsFunc == nil {
		return Error{
			Status:  500,
			Message: "DatasetPermissionsRequestBuilder configuration invalid GetRequestVarsFunc required but was nil",
		}
	}
	return nil
}

// isValid checks that a user or service auth token has been provided.
func (p parameters) isValid() error {
	if p.userAuthToken == "" && p.serviceAuthToken == "" {
		return noUserOrServiceAuthTokenProvidedError
	}
	return nil
}

func (p parameters) isUserRequest() bool {
	return p.userAuthToken != ""
}

func createRequest(url string) (*http.Request, error) {
	httpRequest, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, Error{
			Cause:   err,
			Status:  500,
			Message: "error creating get dataset permissions http request",
		}
	}
	return httpRequest, nil
}
