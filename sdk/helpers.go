package sdk

import (
	"context"
	"net/http"

	dpNetRequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/ONSdigital/log.go/v2/log"
)

// Adds collectID to request header if not empty
// TODO: Add to dp-net?
func addCollectionIDHeader(r *http.Request, collectionID string) {
	if collectionID != "" {
		r.Header.Add(dpNetRequest.CollectionIDHeaderKey, collectionID)
	}
}

// closeResponseBody closes the response body and logs an error if unsuccessful
// TODO: Add to dp-net?
func closeResponseBody(ctx context.Context, resp *http.Response) {
	if resp.Body != nil {
		if err := resp.Body.Close(); err != nil {
			log.Error(ctx, "error closing http response body", err)
		}
	}
}
