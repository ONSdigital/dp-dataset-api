package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	dprequest "github.com/ONSdigital/dp-net/v3/request"
	permissionsAPISDK "github.com/ONSdigital/dp-permissions-api/sdk"

	clientsidentity "github.com/ONSdigital/dp-api-clients-go/v2/identity"
)

// getAuthEntityData returns the EntityData associated with the provided access token
func (api *DatasetAPI) getAuthEntityData(ctx context.Context, accessToken string) (*permissionsAPISDK.EntityData, error) {
	entityData, err := api.authMiddleware.Parse(accessToken)
	if err != nil {
		// check if token is a service token
		resp, errIdentityClient := api.idClient.CheckTokenIdentity(ctx, accessToken, clientsidentity.TokenTypeService)
		if errIdentityClient != nil {
			return nil, errors.Join(err, errIdentityClient, fmt.Errorf("failed to check token identity: %w", errIdentityClient))
		}
		entityData = &permissionsAPISDK.EntityData{UserID: resp.Identifier}
	}
	return entityData, nil
}

// getAccessTokenFromRequest extracts the access token from the Authorization header of the request
func getAccessTokenFromRequest(r *http.Request) string {
	return strings.TrimPrefix(r.Header.Get(dprequest.AuthHeaderKey), dprequest.BearerPrefix)
}
