package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/ONSdigital/dp-api-clients-go/v2/health"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	dpNetRequest "github.com/ONSdigital/dp-net/v2/request"
	"github.com/ONSdigital/log.go/v2/log"
)

const (
	service = "dp-dataset-api"
)

type Client struct {
	hcCli *health.Client
}

// Contains the headers to be added to any request
type Headers struct {
	CollectionID         string
	DownloadServiceToken string
	ServiceToken         string
	UserAccessToken      string
}

// Adds headers to the input request
func (h *Headers) Add(request *http.Request) {
	if h.CollectionID != "" {
		request.Header.Add(dpNetRequest.CollectionIDHeaderKey, h.CollectionID)
	}
	dpNetRequest.AddDownloadServiceTokenHeader(request, h.DownloadServiceToken)
	dpNetRequest.AddFlorenceHeader(request, h.UserAccessToken)
	dpNetRequest.AddServiceTokenHeader(request, h.ServiceToken)
}

// Checker calls topic api health endpoint and returns a check object to the caller
func (c *Client) Checker(ctx context.Context, check *healthcheck.CheckState) error {
	return c.hcCli.Checker(ctx, check)
}

// Health returns the underlying Healthcheck Client for this API client
func (c *Client) Health() *health.Client {
	return c.hcCli
}

// URL returns the URL used by this client
func (c *Client) URL() string {
	return c.hcCli.URL
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

// closeResponseBody closes the response body and logs an error if unsuccessful
func closeResponseBody(ctx context.Context, resp *http.Response) {
	if resp.Body != nil {
		if err := resp.Body.Close(); err != nil {
			log.Error(ctx, "error closing http response body", err)
		}
	}
}

// Takes the input http response and unmarshalls the body to the input target
func unmarshalResponseBody(response *http.Response, target interface{}) (err error) {
	if response.StatusCode != http.StatusOK {
		var errString string
		errResponseReadErr := json.NewDecoder(response.Body).Decode(&errString)
		if errResponseReadErr != nil {
			errString = "Client failed to read DatasetAPI body"
		}
		err = errors.New(errString)
		return err
	}

	b, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, &target)
}
