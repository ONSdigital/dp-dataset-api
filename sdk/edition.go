package sdk

import (
	"context"
	"encoding/json"
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
	resp, err := c.DoAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return edition, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBodyExpectingStringError(resp, &edition)

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

type EditionsDetails struct {
	ID      string  `json:"id"`
	Next    Edition `json:"next"`
	Current Edition `json:"current"`
	Edition
}

type EditionItems struct {
	Items []EditionsDetails `json:"items"`
}

// Edition represents an edition within a dataset
type Edition struct {
	Edition string `json:"edition"`
	ID      string `json:"id"`
	Links   Links  `json:"links"`
	State   string `json:"state"`
}

// Links represent the Links within a dataset model
type Links struct {
	AccessRights  Link `json:"access_rights,omitempty"`
	Dataset       Link `json:"dataset,omitempty"`
	Dimensions    Link `json:"dimensions,omitempty"`
	Edition       Link `json:"edition,omitempty"`
	Editions      Link `json:"editions,omitempty"`
	LatestVersion Link `json:"latest_version,omitempty"`
	Versions      Link `json:"versions,omitempty"`
	Self          Link `json:"self,omitempty"`
	CodeList      Link `json:"code_list,omitempty"`
	Options       Link `json:"options,omitempty"`
	Version       Link `json:"version,omitempty"`
	Code          Link `json:"code,omitempty"`
	Taxonomy      Link `json:"taxonomy,omitempty"`
	Job           Link `json:"job,omitempty"`
}

// Link represents a single link within a dataset model
type Link struct {
	URL string `json:"href"`
	ID  string `json:"id,omitempty"`
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

	if resp.StatusCode != http.StatusOK {
		err = unmarshalResponseBodyExpectingStringError(resp, &editionList)
		return editionList, err
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return editionList, err
	}

	var body map[string]interface{}
	if err = json.Unmarshal(b, &body); err != nil {
		return editionList, nil
	}

	if body["items"] != nil {
		if _, ok := body["items"].([]interface{})[0].(map[string]interface{})["next"]; ok && headers.UserAccessToken != "" || headers.ServiceToken != "" {
			var items []map[string]interface{}
			for _, item := range body["items"].([]interface{}) {
				items = append(items, item.(map[string]interface{})["next"].(map[string]interface{}))
			}
			parentItems := make(map[string]interface{})
			parentItems["items"] = items
			b, err = json.Marshal(parentItems)
			if err != nil {
				return
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
