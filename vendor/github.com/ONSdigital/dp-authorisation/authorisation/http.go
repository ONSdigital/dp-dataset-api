package authorisation

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/ONSdigital/go-ns/common"
	"github.com/ONSdigital/log.go/log"
)

// getPermissionsRequest create a new get permissions http request for the specified service/user/collection ID/dataset ID values.
func (p *PermissionsAuthoriser) getPermissionsRequest(serviceToken string, userToken string, collectionID string, datasetID string) (*http.Request, error) {
	if p.host == "" {
		return nil, Error{
			Status:  500,
			Message: "error creating permissionsList request host not configured",
		}
	}

	url := fmt.Sprintf(gerPermissionsURL, p.host, datasetID, collectionID)
	r, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, Error{
			Cause:   err,
			Status:  500,
			Message: "error making get permissions http request",
		}
	}

	r.Header.Set(common.FlorenceHeaderKey, userToken)
	r.Header.Set(common.AuthHeaderKey, serviceToken)

	return r, nil
}

// getErrorFromResponse handle get permission responses with a non 200 status code.
func getErrorFromResponse(ctx context.Context, resp *http.Response) error {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return Error{
			Status:  500,
			Message: "internal server error failed reading get permissions error response body",
			Cause:   err,
		}
	}

	var entity errorEntity
	if err = json.Unmarshal(body, &entity); err != nil {
		return Error{
			Status:  500,
			Message: "internal server error failed unmarshalling get permissions error response body",
			Cause:   err,
		}
	}

	log.Event(ctx, "get caller permissions request returned an error status", log.Data{
		"status_code": resp.StatusCode,
		"body":        entity,
	})

	permErr := toPermissionError(resp.StatusCode)
	log.Event(ctx, "mapped get permissions error response status to permissions.Error", log.Data{
		"original_error_status":     resp.StatusCode,
		"original_error_message":    entity.Message,
		"permissions_error_status":  permErr.Status,
		"permissions_error_message": permErr.Message,
	})
	return permErr
}

func toPermissionError(status int) (err Error) {
	switch status {
	case 400, 401, 404:
		// treat as caller unauthorized
		return Error{Status: 401, Message: "unauthorized"}
	case 403:
		return Error{Status: 403, Message: "forbidden"}
	default:
		return Error{Status: 500, Message: "internal server error"}
	}
}

// unmarshalPermissions read the response body and unmarshall into a CRUD object
func unmarshalPermissions(reader io.Reader) (*Policy, error) {
	b, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, Error{
			Status:  500,
			Message: "internal server error failed reading get permissions response body",
			Cause:   err,
		}
	}

	var callerPerms callerPermissions
	if err = json.Unmarshal(b, &callerPerms); err != nil {
		return nil, Error{
			Status:  500,
			Message: "internal server error failed marshalling response to permissions",
			Cause:   err,
		}
	}

	if len(callerPerms.List) == 0 {
		return nil, Error{
			Status:  403,
			Message: "forbidden",
		}
	}

	perms := &Policy{}
	for _, p := range callerPerms.List {
		switch p {
		case Create:
			perms.Create = true
		case Read:
			perms.Read = true
		case Update:
			perms.Update = true
		case Delete:
			perms.Delete = true
		}
	}
	return perms, nil
}
