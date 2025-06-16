package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/url"
	"strings"

	"github.com/ONSdigital/dp-dataset-api/models"
)

// Get returns dataset level information for a given dataset id
func (c *Client) GetDataset(ctx context.Context, headers Headers, collectionID, datasetID string) (dataset models.Dataset, err error) {
	dataset = models.Dataset{}

	// Build URI
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

	// Read the response body
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return dataset, err
	}

	// If authenticated, try to extract "next" field from the JSON body
	if headers.ServiceToken != "" || headers.UserAccessToken != "" {
		var bodyMap map[string]interface{}
		if err := json.Unmarshal(b, &bodyMap); err == nil {
			if next, ok := bodyMap["next"]; ok {
				b, err = json.Marshal(next)
				if err != nil {
					return dataset, err
				}
			}
		}
	}

	resp.Body = io.NopCloser(bytes.NewReader(b))

	err = unmarshalResponseBodyExpectingStringError(resp, &dataset)
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
	err = unmarshalResponseBodyExpectingStringError(resp, &dataset)

	return dataset, err
}
