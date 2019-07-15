package mocks

import (
	"net/http"

	"github.com/ONSdigital/dp-authorisation/auth"
)

type CheckPermissionFunc func(handler http.HandlerFunc) http.HandlerFunc

type AuthHandlerMock struct {
	CheckPermissions *PermissionCheckCalls
}

type PermissionCheckCalls struct {
	InvocationCount int
}

func (a AuthHandlerMock) Require(required auth.Permissions, handler http.HandlerFunc) http.HandlerFunc {
	return a.CheckPermissions.checkPermissions(handler)
}

func (c *PermissionCheckCalls) checkPermissions(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c.InvocationCount += 1
		h.ServeHTTP(w, r)
	}
}
