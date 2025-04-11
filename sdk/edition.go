package sdk

import (
	"context"
	"net/url"
	"strconv"

	"github.com/ONSdigital/dp-dataset-api/models"
)

// GetEdition retrieves a single edition document from a given datasetID and edition label
func (c *Client) GetEdition(ctx context.Context, headers Headers, datasetID, editionID string) (edition models.Edition, err error) {
	edition = models.Edition{}
	// Build uri
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID)
	if err != nil {
		return edition, err
	}

	// Make request
	resp, err := c.DoAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return edition, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBody(resp, &edition)

	return edition, err
}

// EditionList represents an object containing a list of paginated versions. This struct is based
// on the `pagination.page` struct which is returned when we call the `api.getEditions` endpoint
type EditionsList struct {
	Items      []models.Edition `json:"items"`
	Count      int              `json:"count"`
	Offset     int              `json:"offset"`
	Limit      int              `json:"limit"`
	TotalCount int              `json:"total_count"`
}

// GetEditions returns all editions for a dataset
func (c *Client) GetEditions(ctx context.Context, headers Headers, datasetID string, queryParams *QueryParams) (editionList EditionsList, err error) {
	editionList = EditionsList{}
	// Build uri
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions")
	if err != nil {
		return editionList, err
	}

	// Add query params to request if valid
	if queryParams != nil {
		if err := queryParams.Validate(); err != nil {
			return editionList, err
		}
		// Add query parameters
		query := uri.Query()
		query.Set("limit", strconv.Itoa(queryParams.Limit))
		query.Set("offset", strconv.Itoa(queryParams.Offset))
		uri.RawQuery = query.Encode()
	}

	// Make request
	resp, err := c.DoAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return editionList, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBody(resp, &editionList)

	return editionList, err
}
