package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/ONSdigital/dp-dataset-api/models"
)

// Get returns dataset level information for a given dataset id
func (c *Client) GetDataset(ctx context.Context, headers Headers, datasetID string) (dataset models.Dataset, err error) {
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

	// If response got errors
	if resp.StatusCode != http.StatusOK {
		err = unmarshalResponseBodyExpectingStringError(resp, &dataset)
		return dataset, err
	}

	// Read the response body (only if status is OK)
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return dataset, err
	}

	var bodyMap map[string]interface{}
	if err := json.Unmarshal(b, &bodyMap); err != nil {
		return dataset, err
	}

	// If authenticated, try to extract "next" field from the JSON body
	if next, ok := bodyMap["next"]; ok && (headers.ServiceToken != "" || headers.UserAccessToken != "") {
		b, err = json.Marshal(next)
		if err != nil {
			return dataset, err
		}
	}

	resp.Body = io.NopCloser(bytes.NewReader(b))
	err = json.Unmarshal(b, &dataset)
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

// DatasetEditionsList represents an object containing a list of paginated dataset editions. This struct is based
// on the `pagination.page` struct which is returned when we call the `api.getDatasetEditions` endpoint
type DatasetEditionsList struct {
	Items      []models.DatasetEdition `json:"items"`
	Count      int                     `json:"count"`
	Offset     int                     `json:"offset"`
	Limit      int                     `json:"limit"`
	TotalCount int                     `json:"total_count"`
}

// GetDatasetEditions returns a list of dataset series that have unpublished versions or match the given state
func (c *Client) GetDatasetEditions(ctx context.Context, headers Headers, queryParams *QueryParams) (datasetEditionsList DatasetEditionsList, err error) {
	datasetEditionsList = DatasetEditionsList{}

	// Build URI
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "dataset-editions")
	if err != nil {
		return datasetEditionsList, err
	}

	// Add query parameters to request if valid
	if queryParams != nil {
		if err := queryParams.Validate(); err != nil {
			return datasetEditionsList, err
		}

		// Add query parameters
		query := url.Values{}
		query.Add("limit", strconv.Itoa(queryParams.Limit))
		query.Add("offset", strconv.Itoa(queryParams.Offset))
		if queryParams.State != "" {
			query.Add("state", queryParams.State)
		}
		uri.RawQuery = query.Encode()
	}

	// Make request
	resp, err := c.DoAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return datasetEditionsList, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBodyExpectingStringError(resp, &datasetEditionsList)

	return datasetEditionsList, err
}
