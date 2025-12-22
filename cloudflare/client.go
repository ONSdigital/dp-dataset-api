package cloudflare

import (
	"errors"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/cloudflare/cloudflare-go/v6/option"
)

// Client is a werapper around the Cloudflare Go SDK client
type Client struct {
	CacheService CacheService
	ZoneID       string
}

// New creates a new Cloudflare client with the provided configuration
func New(cfg *Config) (Clienter, error) {
	if cfg == nil {
		return nil, errors.New("configuration cannot be nil")
	}

	if cfg.BaseURL == "" {
		return nil, errors.New("base URL is required")
	}

	if cfg.APIToken == "" {
		return nil, errors.New("API token is required")
	}

	if cfg.ZoneID == "" {
		return nil, errors.New("zone ID is required")
	}

	client := cloudflare.NewClient(
		option.WithBaseURL(cfg.BaseURL),
		option.WithAPIToken(cfg.APIToken),
	)

	return &Client{
		CacheService: client.Cache,
		ZoneID:       cfg.ZoneID,
	}, nil
}
