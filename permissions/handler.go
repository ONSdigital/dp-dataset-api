package permissions

import (
	"context"
	"net/http"
	"time"

	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/log.go/log"
)

//go:generate moq -out permissions_mocks.go -pkg permissions . Policy Client

const (
	collectionIDHeader = "Collection-Id"
)

var (
	permissionsClient Client
	GetRequestVars    func(r *http.Request) map[string]string
)

func Init() {
	permissionsClient = &ClientImpl{
		HttpClient: http.Client{Timeout: time.Second * 10},
		Host:       "http://localhost:8082",
	}
}

type Policy interface {
	IsSatisfied(ctx context.Context, callerPerms *CallerPermissions, r *http.Request) bool
}

type Client interface {
	Get(serviceToken string, userToken string, collectionID string, datasetID string) (*CallerPermissions, error)
}

func Require(policy Policy, protectedHandlerFunc func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedURI := r.URL.RequestURI()

		serviceAuthToken := r.Header.Get(common.AuthHeaderKey)
		userAuthToken := r.Header.Get(common.FlorenceHeaderKey)
		collectionID := r.Header.Get(collectionIDHeader)
		datasetID := getDatasetID(r)

		callerPermissions, err := permissionsClient.Get(serviceAuthToken, userAuthToken, collectionID, datasetID)

		if err != nil {
			log.Event(r.Context(), "error getting caller permissions", log.Error(err), log.Data{
				"requested_uri": requestedURI,
			})
			w.WriteHeader(500)
			return
		}

		if !policy.IsSatisfied(r.Context(), callerPermissions, r) {
			w.WriteHeader(401)
			return
		}

		protectedHandlerFunc(w, r)
	})
}

func getDatasetID(r *http.Request) string {
	vars := GetRequestVars(r)
	return vars["dataset_id"]
}
