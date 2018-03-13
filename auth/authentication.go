package auth

import (
	"net/http"

	errs "github.com/ONSdigital/dp-dataset-api/apierrors"
	"github.com/ONSdigital/go-ns/log"
)

// Authenticator structure which holds the secret key for validating clients. This will be replaced in the future, after the `thin-slices` has been delivered
type Authenticator struct {
	SecretKey  string
	HeaderName string
}

// Check wraps a HTTP handle. If authentication fails an error code is returned else the HTTP handler is called
func (a *Authenticator) Check(handle func(http.ResponseWriter, *http.Request)) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get(a.HeaderName)
		if key == "" {
			http.Error(w, "Resource not found", http.StatusNotFound)
			log.Error(errs.ErrNoAuthHeader, log.Data{"header": a.HeaderName})
			return
		}
		if key != a.SecretKey {
			http.Error(w, "Resource not found", http.StatusNotFound)
			log.Error(errs.ErrUnauthorised, log.Data{"header": a.HeaderName})
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
		key := r.Header.Get(a.HeaderName)
		if key == a.SecretKey {
			isAuthenticated = true
		}
		handle(w, r, isAuthenticated)
	})
}
