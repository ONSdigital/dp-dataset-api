package authorisation

import (
	"context"
	"net/http"

	"github.com/ONSdigital/log.go/log"
)

const (
	Create permission = "CREATE"
	Read   permission = "READ"
	Update permission = "UPDATE"
	Delete permission = "DELETE"

	gerPermissionsURL = "%s/permissions?dataset_id=%s&collection_id=%s"
)

type Error struct {
	Status  int
	Message string
	Cause   error
}

type permission string

type callerPermissions struct {
	List []permission `json:"permissions"`
}

type errorEntity struct {
	Message string `json:"message"`
}

type HTTPClienter interface {
	Do(ctx context.Context, req *http.Request) (*http.Response, error)
}

type NopAuthoriser struct {}

type PermissionsAuthoriser struct {
	host string
	cli  HTTPClienter
}

// Policy is a definition of permissions required by an endpoint or held by a user/service.
type Policy struct {
	Create bool
	Read   bool
	Update bool
	Delete bool
}

// Satisfied is a authorisation function that checks a caller's permissions contains each of the required permissions.
// Returns nil if the caller has all of the stated required permissions otherwise returns Error with Status 403
func (required *Policy) Satisfied(ctx context.Context, caller *Policy) error {
	missingPermissions := make([]permission, 0)

	if required.Create && !caller.Create {
		missingPermissions = append(missingPermissions, Create)
	}
	if required.Read && !caller.Read {
		missingPermissions = append(missingPermissions, Read)
	}
	if required.Update && !caller.Update {
		missingPermissions = append(missingPermissions, Update)
	}
	if required.Delete && !caller.Delete {
		missingPermissions = append(missingPermissions, Delete)
	}

	if len(missingPermissions) > 0 {
		log.Event(ctx, "action forbidden caller does not process the required permissions", log.Data{
			"required_permissions": required,
			"caller_permissions":   caller,
			"missing_permissions":  missingPermissions,
		})
		return Error{
			Status:  403,
			Message: "action forbidden caller does not process the required permissions",
		}
	}

	log.Event(ctx, "caller has the required permissions to perform the requested action", log.Data{
		"required_permissions": required,
		"caller_permissions":   caller,
	})
	return nil
}

func (e Error) Error() string {
	if e.Cause != nil {
		return e.Cause.Error()
	}
	return e.Message
}
