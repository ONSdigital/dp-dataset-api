package common

import (
	"context"
	"net/http"
	"strconv"

	"github.com/ONSdigital/dp-authorisation/auth"
	"github.com/ONSdigital/dp-dataset-api/mongo"
	"github.com/ONSdigital/log.go/v2/log"
)

var (
	TrueStringified = strconv.FormatBool(true)

	ReadPermission   = auth.Permissions{Read: true}
	CreatePermission = auth.Permissions{Create: true}
	UpdatePermission = auth.Permissions{Update: true}
	DeletePermission = auth.Permissions{Delete: true}
)

// DownloadsGenerator pre generates full file downloads for the specified dataset/edition/version
type DownloadsGenerator interface {
	Generate(ctx context.Context, datasetID, instanceID, edition, version string) error
}

// AuthHandler provides authorisation checks on requests
type AuthHandler interface {
	Require(required auth.Permissions, handler http.HandlerFunc) http.HandlerFunc
}

func SetJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}

func GetIfMatch(r *http.Request) string {
	ifMatch := r.Header.Get("If-Match")
	if ifMatch == "" {
		return mongo.AnyETag
	}
	return ifMatch
}

func SetETag(w http.ResponseWriter, eTag string) {
	w.Header().Add("ETag", eTag)
}

func WriteBody(ctx context.Context, w http.ResponseWriter, b []byte, data log.Data) {
	if _, err := w.Write(b); err != nil {
		log.Error(ctx, "failed to write response body", err, data)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
