package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/ONSdigital/dp-net/v2/request"
	dprequest "github.com/ONSdigital/dp-net/v3/request"
	permissionsAPISDK "github.com/ONSdigital/dp-permissions-api/sdk"

	clientsidentity "github.com/ONSdigital/dp-api-clients-go/v2/identity"
)

// getAuthEntityData returns the EntityData associated with the provided access token
func (api *DatasetAPI) getAuthEntityData(r *http.Request) (*permissionsAPISDK.EntityData, error) {
	accessToken := strings.TrimPrefix(r.Header.Get(request.AuthHeaderKey), request.BearerPrefix)
	entityData, err := api.authMiddleware.Parse(accessToken)
	if err != nil {
		// check service id token is valid
		resp, err := api.idClient.CheckTokenIdentity(r.Context(), accessToken, clientsidentity.TokenTypeService)
		if err != nil {
			return nil, fmt.Errorf("failed to parse access token: %w", err)
		}
		// valid
		entityData = &permissionsAPISDK.EntityData{UserID: resp.Identifier}
	}
	return entityData, nil
}

// getAccessTokenFromRequest extracts the access token from the Authorization header of the request
func getAccessTokenFromRequest(r *http.Request) string {
	return strings.TrimPrefix(r.Header.Get(dprequest.AuthHeaderKey), dprequest.BearerPrefix)
}
