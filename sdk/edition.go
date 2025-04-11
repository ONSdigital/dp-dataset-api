package sdk

import (
	"context"
	"net/url"

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
