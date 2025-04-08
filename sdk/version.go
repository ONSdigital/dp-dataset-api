package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/ONSdigital/dp-dataset-api/models"
	dpNetRequest "github.com/ONSdigital/dp-net/v2/request"
)

const (
	maxIDs = 200
)

// QueryParams represents the possible query parameters that a caller can provide
type QueryParams struct {
	IDs       []string
	IsBasedOn string
	Limit     int
	Offset    int
}

// VersionsList represents an object containing a list of paginated versions. This struct is based
// on the `pagination.page` struct which is returned when we call the `getVersions` endpoint
type VersionsList struct {
	Items      []models.Version `json:"items"`
	Count      int              `json:"count"`
	Offset     int              `json:"offset"`
	Limit      int              `json:"limit"`
	TotalCount int              `json:"total_count"`
}

// Validate validates tht no negative values are provided for limit or offset, and that the length of
// IDs is lower than the maximum
func (q *QueryParams) Validate() error {
	if q.Limit < 0 || q.Offset < 0 {
		return errors.New("negative offsets or limits are not allowed")
	}
	if len(q.IDs) > maxIDs {
		return fmt.Errorf("too many query parameters have been provided. Maximum allowed: %d", maxIDs)
	}
	return nil
}

// GetVersion gets a specific version for an edition from the dataset api
func (c *Client) GetVersion(ctx context.Context, userAccessToken, serviceToken, downloadServiceToken,
	collectionID, datasetID, editionID, versionID string) (version models.Version, err error) {
	version = models.Version{}
	uri, err := url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions", versionID)
	if err != nil {
		return version, err
	}

	req, err := http.NewRequest(http.MethodGet, uri, http.NoBody)
	if err != nil {
		return version, err
	}

	// Add auth headers
	addCollectionIDHeader(req, collectionID)
	dpNetRequest.AddFlorenceHeader(req, userAccessToken)
	dpNetRequest.AddServiceTokenHeader(req, serviceToken)
	dpNetRequest.AddDownloadServiceTokenHeader(req, downloadServiceToken)

	resp, err := c.hcCli.Client.Do(ctx, req)
	if err != nil {
		return version, err
	}

	defer closeResponseBody(ctx, resp)

	if resp.StatusCode != http.StatusOK {
		var errString string
		errResponseReadErr := json.NewDecoder(resp.Body).Decode(&errString)
		if errResponseReadErr != nil {
			errString = "Client failed to read DatasetAPI body"
		}
		err = errors.New(errString)
		return version, err
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return version, err
	}

	err = json.Unmarshal(b, &version)

	return version, err
}

// GetVersions gets all versions for an edition from the dataset api
func (c *Client) GetVersions(ctx context.Context, userAccessToken, serviceToken, downloadServiceToken,
	collectionID, datasetID, editionID string, queryParams *QueryParams) (versionsList VersionsList, err error) {
	versionsList = VersionsList{}

	return versionsList, err
}
