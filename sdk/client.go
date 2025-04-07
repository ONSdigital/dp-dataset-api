package sdk

import (
	"context"

	"github.com/ONSdigital/dp-api-clients-go/v2/health"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
)

const (
	service = "dp-dataset-api"
)

type Client struct {
	hcCli *health.Client
}

// New creates a new instance of Client for the service
func New(datasetAPIUrl string) *Client {
	return &Client{
		hcCli: health.NewClient(service, datasetAPIUrl),
	}
}

// NewWithHealthClient creates a new instance of service API Client, reusing the URL and Clienter
// from the provided healthcheck client
func NewWithHealthClient(hcCli *health.Client) *Client {
	return &Client{
		hcCli: health.NewClientWithClienter(service, hcCli.URL, hcCli.Client),
	}
}

// URL returns the URL used by this client
func (cli *Client) URL() string {
	return cli.hcCli.URL
}

// Health returns the underlying Healthcheck Client for this API client
func (cli *Client) Health() *health.Client {
	return cli.hcCli
}

// Checker calls topic api health endpoint and returns a check object to the caller
func (cli *Client) Checker(ctx context.Context, check *healthcheck.CheckState) error {
	return cli.hcCli.Checker(ctx, check)
}
