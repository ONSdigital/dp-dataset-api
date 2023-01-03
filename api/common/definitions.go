package common

import (
	"context"
	"net/http"
	"strconv"

	"github.com/ONSdigital/dp-authorisation/auth"
)

// DownloadsGenerator pre generates full file downloads for the specified dataset/edition/version
type DownloadsGenerator interface {
	Generate(ctx context.Context, datasetID, instanceID, edition, version string) error
}

// AuthHandler provides authorisation checks on requests
type AuthHandler interface {
	Require(required auth.Permissions, handler http.HandlerFunc) http.HandlerFunc
}

var (
	TrueStringified = strconv.FormatBool(true)

	ReadPermission   = auth.Permissions{Read: true}
	CreatePermission = auth.Permissions{Create: true}
	UpdatePermission = auth.Permissions{Update: true}
	DeletePermission = auth.Permissions{Delete: true}
)

func SetJSONContentType(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}
