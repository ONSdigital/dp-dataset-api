package auth

import (
	"context"
	"net/http"

	"github.com/ONSdigital/log.go/log"
	"github.com/gorilla/mux"
)

// RequireDatasetPermissions is a http.HandlerFunc that wraps another http.HandlerFunc applying an
// authorisation check.
//
// When a request is received the caller's dataset permissions are retrieved from the Permissions API and are compared
// against the required permissions.
//
// If the callers permissions satisfy the requirements authorisation is successful and the
// the wrapped handler is invoked.
//
// If the caller's permissions do not satisfy the permission requirements or there is an issue getting / verifying their
// permissions then the wrapped handler is NOT called and the appropriate HTTP error status is returned.
func RequireDatasetPermissions(required Permissions, handler http.HandlerFunc) http.HandlerFunc {
	return RequirePermissions(required, &DatasetParameterFactory{}, handler)
}

// RequirePermissions is a http.HandlerFunc that wraps another http.HandlerFunc applying an authorisation check. The
// provided ParameterFactory determines the context of the permissions being checking.
//
// When a request is received the caller's permissions are retrieved from the Permissions API and are compared against
// the required permissions.
//
// If the callers permissions satisfy the requirements authorisation is successful and the
// the wrapped handler is invoked.
//
// If the caller's permissions do not satisfy the permission requirements or there is an issue getting / verifying their
// permissions then the wrapped handler is NOT called and the appropriate HTTP error status is returned.
func RequirePermissions(required Permissions, parameterFactory ParameterFactory, handler http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		datasetID := vars[datasetIDKey]
		log.Event(nil, datasetID)

		ctx := req.Context()
		logD := log.Data{"requested_uri": req.URL.RequestURI()}

		parameters, err := parameterFactory.CreateParameters(req)
		if err != nil {
			handleAuthoriseError(req.Context(), err, w, logD)
			return
		}

		callerPermissions, err := permissionsClient.GetCallerPermissions(ctx, parameters)
		if err != nil {
			handleAuthoriseError(req.Context(), err, w, logD)
			return
		}

		err = permissionsVerifier.CheckAuthorisation(ctx, callerPermissions, &required)
		if err != nil {
			handleAuthoriseError(req.Context(), err, w, logD)
			return
		}

		log.Event(req.Context(), "caller authorised to perform requested action", logD)
		handler(w, req)
	})
}

func handleAuthoriseError(ctx context.Context, err error, w http.ResponseWriter, logD log.Data) {
	permErr, ok := err.(Error)
	if ok {
		writeErr(ctx, w, permErr.Status, permErr.Message, logD)
		return
	}
	writeErr(ctx, w, 500, "internal server error", logD)
}

func writeErr(ctx context.Context, w http.ResponseWriter, status int, body string, logD log.Data) {
	w.WriteHeader(status)
	b := []byte(body)
	_, wErr := w.Write(b)
	if wErr != nil {
		w.WriteHeader(500)
		logD["original_err_body"] = body
		logD["original_err_status"] = status

		log.Event(ctx, "internal server error failed writing permissions error to response", log.Error(wErr), logD)
		return
	}
}
