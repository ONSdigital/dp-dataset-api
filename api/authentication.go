package api

import (
	"net/http"
	"github.com/ONSdigital/go-ns/log"
	"errors"
)

// Authenticator structure which holds the secret key for validating clients. This will be replaced in the future, after the `thin-slices` has been delivered
type Authenticator struct {
	secretKey string
	headerName string
}

// NewAuthenticator is created
func NewAuthenticator(key, headerName string) Authenticator {
	return Authenticator{ secretKey: key, headerName: headerName}
}

// Check wraps a HTTP handle. If authentication fails an error code is returned else the HTTP handler is called
func (a *Authenticator) Check(handle func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get(a.headerName)
		if key == "" {
			http.Error(w, "No authentication header provided", http.StatusForbidden)
			log.Error(errors.New("client missing token"), log.Data{"header":a.headerName})
			return
		}
		if key != a.secretKey {
			http.Error(w, "Unauthorised access to API", http.StatusUnauthorized)
			log.Error(errors.New("unauthorised access to API"), log.Data{"header":a.headerName})
			return
		}
		// The request has been authenticated, now run the clients request
		handle(w, r)
	})
}

// ManualCheck a boolean is set and passed to the HTTP handler, its the handler responsibility to set the status code
func (a *Authenticator) ManualCheck(handle func(http.ResponseWriter, *http.Request, bool)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isAuthenticated := false
		key := r.Header.Get(a.headerName)
		if key == a.secretKey {
			isAuthenticated = true
		}
		handle(w, r, isAuthenticated)
	})
}