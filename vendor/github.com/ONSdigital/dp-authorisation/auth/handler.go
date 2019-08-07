package auth

import (
	"context"
	"net/http"

	"github.com/ONSdigital/log.go/log"
)

//go:generate moq -out generated_mocks.go -pkg auth . Clienter Verifier HTTPClienter GetPermissionsRequestBuilder

const (
	// CollectionIDHeader is the collection ID request header key.
	CollectionIDHeader = "Collection-Id"
)

// HTTPClienter is the interface that defines a client for making HTTP requests
type HTTPClienter interface {
	Do(ctx context.Context, req *http.Request) (*http.Response, error)
}

// Clienter is the interface that defines a client for obtaining Permissions from a Permissions API. The Parameters
// argument encapsulates the specifics of the request to make.
type Clienter interface {
	GetPermissions(ctx context.Context, getPermissionsRequest *http.Request) (*Permissions, error)
}

// Verifier is an interface defining a permissions checker. Checks that the caller's permissions satisfy the required
// permissions
type Verifier interface {
	CheckAuthorisation(ctx context.Context, callerPermissions *Permissions, requiredPermissions *Permissions) error
}

type GetPermissionsRequestBuilder interface {
	NewPermissionsRequest(req *http.Request) (getPermissionsRequest *http.Request, err error)
}

// Handler is object providing functionality for applying authorisation checks to http.HandlerFunc's
type Handler struct {
	requestBuilder      GetPermissionsRequestBuilder
	permissionsClient   Clienter
	permissionsVerifier Verifier
}

// LoggerNamespace set the log namespace for auth package logging.
func LoggerNamespace(logNamespace string) {
	log.Namespace = logNamespace
}

// NewHandler construct a new Handler.
//	- requestBuilder an implementation of GetPermissionsRequestBuilder that creates Permissions API requests from the inbound http request.
//	- permissionsClient is a client for communicating with the permissions API.
//	- permissionsVerifier is an object that checks a caller's permissions satisfy the permissions requirements.
func NewHandler(requestBuilder GetPermissionsRequestBuilder, permissionsClient Clienter, permissionsVerifier Verifier) *Handler {
	return &Handler{
		requestBuilder:      requestBuilder,
		permissionsClient:   permissionsClient,
		permissionsVerifier: permissionsVerifier,
	}
}

// Require is a http.HandlerFunc that wraps another http.HandlerFunc applying an authorisation check. The
// provided GetPermissionsRequestBuilder determines what Permissions API request to create from the inbound http request.
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

		getPermissionsRequest, err := h.requestBuilder.NewPermissionsRequest(req)
		if err != nil {
			handleAuthoriseError(req.Context(), err, w, logD)
			return
		}

		permissions, err := h.permissionsClient.GetPermissions(ctx, getPermissionsRequest)
		if err != nil {
			handleAuthoriseError(req.Context(), err, w, logD)
			return
		}

		err = h.permissionsVerifier.CheckAuthorisation(ctx, permissions, &required)
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
