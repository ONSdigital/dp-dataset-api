package sdk

import (
	"context"
	"net/url"
	"strings"

	"github.com/ONSdigital/dp-dataset-api/models"
)

// Get returns dataset level information for a given dataset id
func (c *Client) GetDataset(ctx context.Context, headers Headers, collectionID, datasetID string) (dataset models.Dataset, err error) {
	dataset = models.Dataset{}
	// Build uri
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID)
	if err != nil {
		return dataset, err
	}

	// Make request
	resp, err := c.DoAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return dataset, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBody(resp, &dataset)

	return dataset, err
}

// GetDatasetByPath returns dataset level information for a given dataset path
func (c *Client) GetDatasetByPath(ctx context.Context, headers Headers, path string) (dataset models.Dataset, err error) {
	dataset = models.Dataset{}
	// Build uri
	uri := &url.URL{}
	trimmedPath := strings.Trim(path, "/")
	uri.Path, err = url.JoinPath(c.hcCli.URL, trimmedPath)
	if err != nil {
		return dataset, err
	}

	// Make request
	resp, err := c.DoAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return dataset, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBody(resp, &dataset)

	return dataset, err
}
