package cloudflare

import "time"

// Config holds configuration details for Cloudflare API access
type Config struct {
	APIToken string        `envconfig:"CLOUDFLARE_API_TOKEN" json:"-"`
	ZoneID   string        `envconfig:"CLOUDFLARE_ZONE_ID"`
	BaseURL  string        `envconfig:"CLOUDFLARE_BASE_URL"`
	Timeout  time.Duration `envconfig:"CLOUDFLARE_TIMEOUT"`
}

// NewDefaultConfig returns a Config struct with default test values.
// This is compatible with dis-cloudflare-stub:
// https://github.com/ONSdigital/dp-compose/tree/main/v2/stubs/dis-cloudflare-stub
func NewDefaultConfig() *Config {
	return &Config{
		APIToken: "test-token",
		ZoneID:   "a1b2c3d4e5f6g7h8i9j1k2l3m4n5o6p7",
		BaseURL:  "http://localhost:30200",
		Timeout:  30 * time.Second,
	}
}

// Validate checks that all required fields are set in the Config.
func (cfg *Config) Validate() error {
	if cfg == nil {
		return errNilConfig
	}
	if cfg.BaseURL == "" {
		return errMissingBaseURL
	}
	if cfg.APIToken == "" {
		return errMissingAPIToken
	}
	if cfg.ZoneID == "" {
		return errMissingZoneID
	}
	if cfg.Timeout <= 0 {
		return errInvalidTimeout
	}
	return nil
}
