package cloudflare

// Config holds configuration details for Cloudflare API access
type Config struct {
	APIToken string `envconfig:"CLOUDFLARE_API_TOKEN"`
	ZoneID   string `envconfig:"CLOUDFLARE_ZONE_ID"`
	BaseURL  string `envconfig:"CLOUDFLARE_BASE_URL"`
}

// NewDefaultConfig returns a Config struct with default test values.
// This is compatible with dis-cloudflare-stub:
// https://github.com/ONSdigital/dp-compose/tree/main/v2/stubs/dis-cloudflare-stub
func NewDefaultConfig() *Config {
	return &Config{
		APIToken: "test-token",
		ZoneID:   "a1b2c3d4e5f6g7h8i9j1k2l3m4n5o6p7",
		BaseURL:  "http://localhost:30200",
	}
}
