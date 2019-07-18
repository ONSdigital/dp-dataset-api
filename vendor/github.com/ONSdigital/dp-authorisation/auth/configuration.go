package auth

import (
	"context"
	"net/http"

	"github.com/ONSdigital/log.go/log"
)

//go:generate moq -out generated_mocks.go -pkg auth . Clienter Verifier HTTPClienter Parameters ParameterFactory

const (
	// CollectionIDHeader is the collection ID request header key.
	CollectionIDHeader = "Collection-Id"
)

var (
	getRequestVars      func(r *http.Request) map[string]string
	datasetIDKey        string
)

// GetRequestVarsFunc is a utility function for retrieving URL path parameters and request headers from a HTTP Request
type GetRequestVarsFunc func(r *http.Request) map[string]string

// HTTPClienter is the interface that defines a client for making HTTP requests
type HTTPClienter interface {
	Do(ctx context.Context, req *http.Request) (*http.Response, error)
}

// Clienter is the interface that defines a client for obtaining Permissions from a Permissions API. The Parameters
// argument encapsulates the specifics of the request to make.
type Clienter interface {
	GetCallerPermissions(ctx context.Context, params Parameters) (callerPermissions *Permissions, err error)
}

// Verifier is an interface defining a permissions checker. Checks that the caller's permissions satisfy the required
// permissions
type Verifier interface {
	CheckAuthorisation(ctx context.Context, callerPermissions *Permissions, requiredPermissions *Permissions) error
}

// ParameterFactory interface defining a parameter factory. ParameterFactory creates a new Parameters instance from a
// HTTP request
type ParameterFactory interface {
	CreateParameters(req *http.Request) (Parameters, error)
}

// Configure is an initialise function for the auth package.
// 	- DatasetIDKey is the URL placeholder name for dataset ID variable
// 	- GetRequestVarsFunc is a function for getting URL path variables and headers form a HTTP request.
// 	- logNamespace is the namespace to use for auth package logging.
func Configure(DatasetIDKey string, GetRequestVarsFunc GetRequestVarsFunc, logNamespace string) {
	log.Namespace = logNamespace
	datasetIDKey = DatasetIDKey
	getRequestVars = GetRequestVarsFunc
}

