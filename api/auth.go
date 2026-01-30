package api

import (
	"fmt"
	"net/http"
	"strings"

	dprequest "github.com/ONSdigital/dp-net/v3/request"
	permissionsAPISDK "github.com/ONSdigital/dp-permissions-api/sdk"
)

// getAuthEntityData returns the EntityData associated with the provided access token
func (api *DatasetAPI) getAuthEntityData(accessToken string) (*permissionsAPISDK.EntityData, error) {
	entityData, err := api.authMiddleware.Parse(accessToken)
	if err != nil {
		return nil, fmt.Errorf("failed to parse access token: %w", err)
	}

	return entityData, nil
}

// getAccessTokenFromRequest extracts the access token from the Authorization header of the request
func getAccessTokenFromRequest(r *http.Request) string {
	return strings.TrimPrefix(r.Header.Get(dprequest.AuthHeaderKey), dprequest.BearerPrefix)
}
