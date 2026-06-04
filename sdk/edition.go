package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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
	resp, err := c.doAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return edition, err
	}

	defer closeResponseBody(ctx, resp)

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return edition, err
	}

	if resp.StatusCode != http.StatusOK {
		return edition, fmt.Errorf("did not receive success response. received status %d, response body: %s", resp.StatusCode, string(b))
	}

	// Response could be either an Edition or an EditionUpdate.
	var bodyMap map[string]interface{}
	if err := json.Unmarshal(b, &bodyMap); err != nil {
		return edition, err
	}

	// If the response is an EditionUpdate, return the "next" edition.
	if next, ok := bodyMap["next"]; ok {
		b, err = json.Marshal(next)
		if err != nil {
			return edition, err
		}
	}

	err = json.Unmarshal(b, &edition)

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

// GetEditions returns a paginated list of editions for a dataset
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
	resp, err := c.doAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return editionList, err
	}

	defer closeResponseBody(ctx, resp)

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return editionList, err
	}

	if resp.StatusCode != http.StatusOK {
		return editionList, fmt.Errorf("did not receive success response. received status %d, response body: %s", resp.StatusCode, string(b))
	}

	var body map[string]interface{}
	if err = json.Unmarshal(b, &body); err != nil {
		return editionList, nil
	}

	if body["items"] != nil {
		if _, ok := body["items"].([]interface{})[0].(map[string]interface{})["next"]; ok {
			var items []map[string]interface{}
			for _, item := range body["items"].([]interface{}) {
				items = append(items, item.(map[string]interface{})["next"].(map[string]interface{}))
			}
			parentItems := make(map[string]interface{})
			parentItems["items"] = items
			b, err = json.Marshal(parentItems)
			if err != nil {
				return editionList, err
			}
		}
	}

	editions := struct {
		Items []models.Edition `json:"items"`
	}{}
	err = json.Unmarshal(b, &editions)
	editionList.Items = editions.Items

	return editionList, err
}
