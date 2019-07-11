package auth

import (
	"fmt"
	"net/http"

	"github.com/ONSdigital/go-ns/common"
)

// Parameters is an interface that defines the specific context of auth check is being made. The details of the fields
// required and the structure of the HTTP request to get the caller permissions is encapsulated by implementing struct.
type Parameters interface {
	CreateGetPermissionsRequest(host string) (*http.Request, error)
}

// UserDatasetParameters is an implementation of Parameters. Requires a user auth token, collection ID and dataset ID
// and checks a user is authorised to access the specified dataset in the specified collection.
type UserDatasetParameters struct {
	UserToken    string
	CollectionID string
	DatasetID    string
}

// ServiceDatasetParameters is an implementation of Parameters. Requires a service account auth token and dataset ID.
// Checks a service account is authorised to access the specified dataset.
type ServiceDatasetParameters struct {
	ServiceToken string
	DatasetID    string
}

// DatasetParameterFactory is an implementation of ParameterFactory. Creates Parameters for requesting user & service
// account permissions for a CMD dataset.
type DatasetParameterFactory struct{}

// CreateParameters fulfilling the ParameterFactory interface. Generates:
// 	- A UserDatasetParameters instance if the request contains a user auth token header.
// 	- Or a ServiceDatasetParameters instance if the request contains the service account auth token header.
// If neither header is present then returns noUserOrServiceAuthTokenProvidedError
func (f *DatasetParameterFactory) CreateParameters(req *http.Request) (Parameters, error) {
	userAuthToken := req.Header.Get(common.FlorenceHeaderKey)
	serviceAuthToken := req.Header.Get(common.AuthHeaderKey)
	collectionID := req.Header.Get(CollectionIDHeader)
	datasetID := getRequestVars(req)[datasetIDKey]

	if userAuthToken != "" {
		return newUserDatasetParameters(userAuthToken, collectionID, datasetID), nil
	}

	if serviceAuthToken != "" {
		return newServiceParameters(serviceAuthToken, datasetID), nil
	}

	return nil, noUserOrServiceAuthTokenProvidedError
}

func newUserDatasetParameters(userToken string, collectionID string, datasetID string) Parameters {
	return &UserDatasetParameters{
		UserToken:    userToken,
		CollectionID: collectionID,
		DatasetID:    datasetID,
	}
}

func newServiceParameters(serviceToken string, datasetID string) Parameters {
	return &ServiceDatasetParameters{
		ServiceToken: serviceToken,
		DatasetID:    datasetID,
	}
}

// CreateGetPermissionsRequest fulfilling the Parameters interface - creates a Permissions API request to get user
// dataset permissions.
func (params *UserDatasetParameters) CreateGetPermissionsRequest(host string) (*http.Request, error) {
	if host == "" {
		return nil, hostRequiredButEmptyError
	}

	url := fmt.Sprintf(userDatasetPermissionsURL, host, params.DatasetID, params.CollectionID)
	httpRequest, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, Error{
			Cause:   err,
			Status:  500,
			Message: "error creating new get user dataset permissions http request",
		}
	}

	httpRequest.Header.Set(common.FlorenceHeaderKey, params.UserToken)
	return httpRequest, nil
}

// CreateGetPermissionsRequest fulfilling the Parameters interface - creates a Permissions API request to get service
// account dataset permissions.
func (params *ServiceDatasetParameters) CreateGetPermissionsRequest(host string) (*http.Request, error) {
	if host == "" {
		return nil, hostRequiredButEmptyError
	}

	url := fmt.Sprintf(serviceDatasetPermissionsURL, host, params.DatasetID)
	r, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, Error{
			Cause:   err,
			Status:  500,
			Message: "error creating new get service dataset permissions http request",
		}
	}

	r.Header.Set(common.AuthHeaderKey, params.ServiceToken)
	return r, nil
}
