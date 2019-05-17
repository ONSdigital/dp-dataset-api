package auth

import (
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/permissions"
	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/log.go/log"
)

//go:generate moq -out auth_mocks.go -pkg auth . PermissionAuthenticator

const (
	collectionIDHeader = "Collection-Id"
	datasetIDParam     = "dataset_id"
)

var (
	getRequestVars func(r *http.Request) map[string]string
	authenticator  PermissionAuthenticator
)

func Init(GetRequestVarsFunc func(r *http.Request) map[string]string, PermissionsAuthenticator PermissionAuthenticator) {
	getRequestVars = GetRequestVarsFunc
	authenticator = PermissionsAuthenticator
}

type PermissionAuthenticator interface {
	Check(required permissions.Permissions, serviceToken string, userToken string, collectionID string, datasetID string) (bool, error)
}

func Require(required permissions.Permissions, endpoint func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedURI := r.URL.RequestURI()

		serviceAuthToken := r.Header.Get(common.AuthHeaderKey)
		userAuthToken := r.Header.Get(common.FlorenceHeaderKey)
		collectionID := r.Header.Get(collectionIDHeader)
		datasetID := getDatasetID(r)

		authorized, err := authenticator.Check(required, serviceAuthToken, userAuthToken, collectionID, datasetID)
		if err != nil {
			log.Event(r.Context(), "error authenticating caller permissions", log.Error(err), log.Data{
				"requested_uri": requestedURI,
			})
			w.WriteHeader(500)
			return
		}

		if !authorized {
			w.WriteHeader(401)
			return
		}

		endpoint(w, r)
	})
}

func getDatasetID(r *http.Request) string {
	return getRequestVars(r)[datasetIDParam]
}
