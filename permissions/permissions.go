package permissions

import (
	"context"
	"net/http"

	"github.com/ONSdigital/log.go/log"
)

type Permission string

type PolicyImpl struct {
	RequiredPermissions []Permission `json:"permissions"`
}

type CallerPermissions struct {
	Permissions []Permission `json:"permissions"`
}

const (
	CREATE Permission = "CREATE"
	READ   Permission = "READ"
	UPDATE Permission = "UPDATE"
	DELETE Permission = "DELETE"
)

type Error struct {
	Message string
	Cause   error
}

func NewPolicy(requiredPermissions ...Permission) *PolicyImpl {
	required := make([]Permission, 0)

	for _, p := range requiredPermissions {
		required = append(required, p)
	}

	return &PolicyImpl{RequiredPermissions: required}
}

func (p *PolicyImpl) IsSatisfied(ctx context.Context, callerPerms *CallerPermissions, r *http.Request) bool {
	callerPermissions := make(map[Permission]bool)
	for _, p := range callerPerms.Permissions {
		callerPermissions[p] = true
	}

	for _, required := range p.RequiredPermissions {
		if _, present := callerPermissions[required]; !present {
			log.Event(ctx, "access denied caller permissions did not satisfy the permission policy for this endpoint",
				log.Data{
					"requested_uri": r.URL.RequestURI(),
					"method":        r.Method,
					"required":      p.RequiredPermissions,
					"permitted":     callerPerms.Permissions,
				})
			return false
		}
	}

	log.Event(ctx, "caller permissions satisfy permissions policy endpoint access granted", log.Data{
		"requested_uri": r.URL.RequestURI(),
		"method":        r.Method,
	})
	return true
}
