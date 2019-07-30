package auth

import (
	"context"
	"net/http"

	"github.com/ONSdigital/log.go/log"
)

// Handler is object providing functionality for applying authorisation checks to http.HandlerFunc's
type Handler struct {
	parameterFactory    ParameterFactory
	permissionsClient   Clienter
	permissionsVerifier Verifier
}

// NewHandler construct a new Handler.
//	- parameterFactory is a factory object which generates Parameters object from a HTTP request.
//	- permissionsClient is a client for communicating with the permissions API.
//	- permissionsVerifier is an object that checks a caller's permissions satisfy the permissions requirements.
func NewHandler(parameterFactory ParameterFactory, permissionsClient Clienter, permissionsVerifier Verifier) *Handler {
	return &Handler{
		parameterFactory:    parameterFactory,
		permissionsClient:   permissionsClient,
		permissionsVerifier: permissionsVerifier,
	}
}

// Require is a http.HandlerFunc that wraps another http.HandlerFunc applying an authorisation check. The
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
func (h *Handler) Require(required Permissions, handler http.HandlerFunc) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		logD := log.Data{"requested_uri": req.URL.RequestURI()}

		parameters, err := h.parameterFactory.CreateParameters(req)
		if err != nil {
			handleAuthoriseError(req.Context(), err, w, logD)
			return
		}

		callerPermissions, err := h.permissionsClient.GetCallerPermissions(ctx, parameters)
		if err != nil {
			handleAuthoriseError(req.Context(), err, w, logD)
			return
		}

		err = h.permissionsVerifier.CheckAuthorisation(ctx, callerPermissions, &required)
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
