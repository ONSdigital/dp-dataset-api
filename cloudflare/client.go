package cloudflare

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ONSdigital/log.go/v2/log"
	"github.com/cloudflare/cloudflare-go"
)

// Clienter defines the interface for Cloudflare cache operations
type Clienter interface {
	PurgeCacheByPrefix(ctx context.Context, datasetID, editionID string) error
}

// Client wraps the Cloudflare API client
type Client struct {
	api        *cloudflare.API
	zoneID     string
	baseURL    string
	httpClient *http.Client
	apiToken   string
}

// New creates a new Cloudflare client
func New(apiToken, zoneID string, useSDK bool, baseURL ...string) (*Client, error) {
	if apiToken == "" {
		return nil, fmt.Errorf("cloudflare API token is required")
	}
	if zoneID == "" {
		return nil, fmt.Errorf("cloudflare zone ID is required")
	}

	// if SDK disabled for local testing with the cloudflare stub, use the HTTP client
	if !useSDK && len(baseURL) > 0 && baseURL[0] != "" {
		return &Client{
			api:        nil,
			zoneID:     zoneID,
			baseURL:    baseURL[0],
			apiToken:   apiToken,
			httpClient: &http.Client{Timeout: 10 * time.Second},
		}, nil
	}

	// use real Cloudflare SDK
	api, err := cloudflare.NewWithAPIToken(apiToken)
	if err != nil {
		return nil, fmt.Errorf("failed to create cloudflare client: %w", err)
	}

	return &Client{
		api:    api,
		zoneID: zoneID,
	}, nil
}

// PurgeCacheByPrefix purges the Cloudflare cache for dataset-related URLs
func (c *Client) PurgeCacheByPrefix(ctx context.Context, datasetID, editionID string) error {
	prefixes := buildPrefixes(datasetID, editionID)

	logData := log.Data{
		"dataset_id": datasetID,
		"edition":    editionID,
		"prefixes":   prefixes,
	}
	log.Info(ctx, "purging cloudflare cache", logData)

	// local mock
	if c.baseURL != "" {
		return c.purgeCacheViaHTTP(ctx, prefixes)
	}

	// for sdk
	return c.purgeCacheViaSDK(ctx, prefixes)
}

// purgeCacheViaSDK uses the cloudflare sdk
func (c *Client) purgeCacheViaSDK(ctx context.Context, prefixes []string) error {
	params := cloudflare.PurgeCacheRequest{
		Prefixes: prefixes,
	}

	_, err := c.api.PurgeCache(ctx, c.zoneID, params)
	if err != nil {
		return fmt.Errorf("failed to purge cache: %w", err)
	}

	return nil
}

// purgeCacheViaHTTP makes direct HTTP call for local cloudflare stub
func (c *Client) purgeCacheViaHTTP(ctx context.Context, prefixes []string) error {
	url := fmt.Sprintf("%s/zones/%s/purge_cache", c.baseURL, c.zoneID)

	payload := map[string]interface{}{
		"prefixes": prefixes,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiToken))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("purge failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

func buildPrefixes(datasetID, editionID string) []string {
	return []string{
		fmt.Sprintf("www.ons.gov.uk/datasets/%s", datasetID),
		fmt.Sprintf("www.ons.gov.uk/datasets/%s/editions", datasetID),
		fmt.Sprintf("www.ons.gov.uk/datasets/%s/editions/%s/versions", datasetID, editionID),
		fmt.Sprintf("api.beta.ons.gov.uk/v1/datasets/%s", datasetID),
		fmt.Sprintf("api.beta.ons.gov.uk/v1/datasets/%s/editions", datasetID),
		fmt.Sprintf("api.beta.ons.gov.uk/v1/datasets/%s/editions/%s/versions", datasetID, editionID),
	}
}
