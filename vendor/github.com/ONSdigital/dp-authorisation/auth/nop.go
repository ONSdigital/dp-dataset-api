package auth

import (
	"net/http"

	"github.com/ONSdigital/log.go/log"
)

// NopHandler is a Nop impl of auth.Handler which simply logs that it has been invoked and returns the wrapped handlerFunc.
type NopHandler struct{}

func (h *NopHandler) Require(required Permissions, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Event(r.Context(), "executing NopHandler.Require", log.Data{
			"uri":                  r.URL.Path,
			"method":               r.Method,
			"required_permissions": required,
		})
		handler.ServeHTTP(w, r)
	}
}
