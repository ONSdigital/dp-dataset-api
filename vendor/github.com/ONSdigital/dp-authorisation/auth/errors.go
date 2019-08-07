package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/ONSdigital/log.go/log"
)

var (
	requestRequiredButNilError = Error{
		Status:  500,
		Message: "error creating get permissions request http.Request required but was nil",
	}

	noUserOrServiceAuthTokenProvidedError = Error{
		Message: "invalid request require user or service auth token but none provide",
		Status:  400,
	}

	checkAuthorisationForbiddenError = Error{
		Message: "access denied caller does not have the required permissions to perform this action",
		Status:  403,
	}

	getPermissionsResponseBodyNilError = Error{
		Status:  500,
		Message: "internal server error response body was nil",
	}

	getPermissionsUnauthorizedError = Error{
		Status:  401,
		Message: "error making get permissions request: unauthorized",
	}

	getPermissionsRequestNilError = Error{
		Status:  500,
		Message: "internal server error expected get permissions http request but was nil",
	}
)

// Error is an auth package implementation of Error.
type Error struct {
	Status  int
	Message string
	Cause   error
}

// Error get the error message & cause if not nil
func (e Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %s", e.Message, e.Cause.Error())
	}
	return e.Message
}

func handleGetPermissionsErrorResponse(ctx context.Context, body io.Reader, status int) error {
	errorEntity, err := getErrorEntityFromResponse(body)
	if err != nil {
		log.Event(
			ctx, "error unmarshalling get permissions error response. Returning 401 status as unable to verify caller permissions", log.Error(err), log.Data{
				"get_permissions_status_code": status,
			})
		return getPermissionsUnauthorizedError
	}

	log.Event(ctx, "get permissions request returned error status. Returning 401 status as unable to verify caller permissions",
		log.Data{
			"get_permissions_status_code": status,
			"get_permissions_body":        errorEntity,
		})

	return getPermissionsUnauthorizedError
}

func getErrorEntityFromResponse(body io.Reader) (*errorEntity, error) {
	jBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, Error{
			Status:  500,
			Message: "internal server error failed reading get permissions error response body",
			Cause:   err,
		}
	}

	var entity errorEntity
	if err = json.Unmarshal(jBytes, &entity); err != nil {
		return nil, Error{
			Status:  500,
			Message: "internal server error failed unmarshalling get permissions error response body",
			Cause:   err,
		}
	}

	return &entity, nil
}
