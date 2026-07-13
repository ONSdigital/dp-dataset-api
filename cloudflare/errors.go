package cloudflare

import "errors"

// Predefined errors used within the cloudflare package
var (
	// configuration errors
	errNilConfig       = errors.New("cloudflare configuration is nil")
	errMissingBaseURL  = errors.New("cloudflare base URL is required")
	errMissingAPIToken = errors.New("cloudflare API token is required")
	errMissingZoneID   = errors.New("cloudflare zone ID is required")
	errInvalidTimeout  = errors.New("cloudflare timeout must be greater than 0")
)
