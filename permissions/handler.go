package permissions

import (
	"net/http"

	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/log.go/log"
)

//go:generate moq -out permissions_mocks.go -pkg permissions . Permissions

const (
	collectionIDHeader = "Collection-Id"
)

var (
	GetRequestVars func(r *http.Request) map[string]string
)

type Permissions interface {
	Check(serviceToken string, userToken string, collectionID string, datasetID string) (bool, error)
}

func Require(p Permissions, endpoint func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedURI := r.URL.RequestURI()

		serviceAuthToken := r.Header.Get(common.AuthHeaderKey)
		userAuthToken := r.Header.Get(common.FlorenceHeaderKey)
		collectionID := r.Header.Get(collectionIDHeader)
		datasetID := getDatasetID(r)

		authorized, err := p.Check(serviceAuthToken, userAuthToken, collectionID, datasetID)
		if err != nil {
			log.Event(r.Context(), "error getting caller permissions", log.Error(err), log.Data{
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
	vars := GetRequestVars(r)
	return vars["dataset_id"]
}
