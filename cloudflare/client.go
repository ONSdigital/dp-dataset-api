package cloudflare

import (
	"time"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/cloudflare/cloudflare-go/v6/option"
)

// Client is a wrapper around the Cloudflare Go SDK client
type Client struct {
	CacheService CacheService
	ZoneID       string
	timeout      time.Duration
}

// New creates a new Cloudflare client with the provided configuration.
func New(cfg *Config) (Clienter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	client := cloudflare.NewClient(
		option.WithBaseURL(cfg.BaseURL),
		option.WithAPIToken(cfg.APIToken),
	)

	return &Client{
		CacheService: client.Cache,
		ZoneID:       cfg.ZoneID,
		timeout:      cfg.Timeout,
	}, nil
}

// GetTimeout returns the timeout duration for Cloudflare API requests.
func (c *Client) GetTimeout() time.Duration {
	return c.timeout
}
