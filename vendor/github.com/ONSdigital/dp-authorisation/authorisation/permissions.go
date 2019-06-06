package authorisation

import (
	"context"

	"github.com/ONSdigital/log.go/log"
)

// NewAuthoriser is a constructor function for creating a new instance of PermissionsAuthoriser
func NewAuthoriser(host string, httpClient HTTPClienter) *PermissionsAuthoriser {
	return &PermissionsAuthoriser{
		host: host,
		cli:  httpClient,
	}
}

// Allow is an authorisation function. Given a policy, service and or user token, a collection ID and dataset ID determined
// if the caller has the necessary permissions to perform the requested action. Returns and error if their permissions
// are insufficient or if there is an error attempting to check. If the caller has the necessary permissions return nil.
func (a *PermissionsAuthoriser) Allow(ctx context.Context, required Policy, serviceToken string, userToken string, collectionID string, datasetID string) error {
	r, err := a.getPermissionsRequest(serviceToken, userToken, collectionID, datasetID)
	if err != nil {
		return Error{
			Status:  500,
			Message: "error making get permissions http request",
			Cause:   err,
		}
	}

	resp, err := a.cli.Do(ctx, r)
	if err != nil {
		return Error{
			Status:  500,
			Message: "get permissions request returned an error",
			Cause:   err,
		}
	}

	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Event(ctx, "error closing response body", log.Error(err))
		}
	}()

	if resp.StatusCode != 200 {
		return getErrorFromResponse(r.Context(), resp)
	}

	callerPerms, err := unmarshalPermissions(resp.Body)
	if err != nil {
		return err
	}

	return required.Satisfied(ctx, callerPerms)
}

// Allow NOP implementation. No authorisation check is applied, always returns nil
func (nop *NopAuthoriser) Allow(ctx context.Context, required Policy, serviceToken string, userToken string, collectionID string, datasetID string) error {
	log.Event(ctx, "NopAuthoriser.Allow configured no authorisation check applied to caller")
	return nil
}
