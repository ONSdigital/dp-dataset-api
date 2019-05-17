package auth

import (
	"net/http"

	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/log.go/log"
)

//go:generate moq -out authtest/auth_mocks.go -pkg authtest . PermissionAuthenticator

const (
	CollectionIDHeader = "Collection-Id"
	DatasetIDParam     = "dataset_id"
)

var (
	getRequestVars func(r *http.Request) map[string]string
	authenticator  PermissionAuthenticator
)

// Init set up function for the auth pkg. Requires a function for getting request parameters and a
// PermissionsAuthenticator implementation
func Init(GetRequestVarsFunc func(r *http.Request) map[string]string, PermissionsAuthenticator PermissionAuthenticator) {
	getRequestVars = GetRequestVarsFunc
	authenticator = PermissionsAuthenticator
}

// CRUD a representation of CRUD permissions required to access an endpoint.
type CRUD interface {
	IsCreate() bool
	IsRead() bool
	IsUpdate() bool
	IsDelete() bool
}

type PermissionAuthenticator interface {
	Check(required CRUD, serviceToken string, userToken string, collectionID string, datasetID string) (int, error)
}

// Require is a http.HandlerFunc that verifies the caller holds the required permissions for the wrapped
// http.HandlerFunc If the caller has all of the required permissions then the request will continue to the wrapped
// handlerFunc. If the caller does not have all the required permissions then the the request is rejected with the
// appropriate http status and the wrapped handler is not invoked. If there is an error whilst attempting to check the
// callers permissions then a 500 status is returned and the wrapped handler is not invoked.
func Require(required CRUD, endpoint func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedURI := r.URL.RequestURI()

		serviceAuthToken := r.Header.Get(common.AuthHeaderKey)
		userAuthToken := r.Header.Get(common.FlorenceHeaderKey)
		collectionID := r.Header.Get(CollectionIDHeader)
		datasetID := getDatasetID(r)

		authStatus, err := authenticator.Check(required, serviceAuthToken, userAuthToken, collectionID, datasetID)
		if err != nil {
			log.Event(r.Context(), "error authenticating caller permissions", log.Error(err), log.Data{
				"requested_uri": requestedURI,
			})
			w.WriteHeader(500)
			return
		}

		if authStatus != 200 {
			w.WriteHeader(authStatus)
			return
		}

		endpoint(w, r)
	})
}

func getDatasetID(r *http.Request) string {
	return getRequestVars(r)[DatasetIDParam]
}
