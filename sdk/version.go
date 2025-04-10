package sdk

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/ONSdigital/dp-dataset-api/models"
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
func (c *Client) GetVersion(ctx context.Context, headers Headers, datasetID, editionID, versionID string) (version models.Version, err error) {
	version = models.Version{}
	uri, err := url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions", versionID)
	if err != nil {
		return version, err
	}

	req, err := http.NewRequest(http.MethodGet, uri, http.NoBody)
	if err != nil {
		return version, err
	}

	// Add auth headers to the request
	headers.Add(req)

	resp, err := c.hcCli.Client.Do(ctx, req)
	if err != nil {
		return version, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshall the response body to target
	err = unmarshalResponseBody(resp, &version)

	return version, err
}

// VersionDimensions represent a list of versionDimension
type VersionDimensionsList struct {
	Items []models.Dimension
}

// GetVersionDimensions will return a list of dimensions for a given version of a dataset
func (c *Client) GetVersionDimensions(ctx context.Context, headers Headers, datasetID, editionID, versionID string) (versionDimensionsList VersionDimensionsList, err error) {
	versionDimensionsList = VersionDimensionsList{}
	// Build uri
	uri, err := url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions", versionID, "dimensions")
	if err != nil {
		return versionDimensionsList, err
	}

	// Create new request
	req, err := http.NewRequest(http.MethodGet, uri, http.NoBody)
	if err != nil {
		return versionDimensionsList, err
	}

	// Add auth headers to the request
	headers.Add(req)

	resp, err := c.hcCli.Client.Do(ctx, req)
	if err != nil {
		return versionDimensionsList, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshall the response body to target
	err = unmarshalResponseBody(resp, &versionDimensionsList)

	return versionDimensionsList, err
}

// GetVersionMetadata returns the metadata for a given dataset id, edition and version
func (c *Client) GetVersionMetadata(ctx context.Context, headers Headers, datasetID, editionID, versionID string) (metadata models.Metadata, err error) {
	metadata = models.Metadata{}
	uri, err := url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions", versionID, "metadata")
	if err != nil {
		return metadata, err
	}

	req, err := http.NewRequest(http.MethodGet, uri, http.NoBody)
	if err != nil {
		return metadata, err
	}

	// Add auth headers to the request
	headers.Add(req)

	resp, err := c.hcCli.Client.Do(ctx, req)
	if err != nil {
		return metadata, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshall the response body to target
	err = unmarshalResponseBody(resp, &metadata)

	return metadata, err
}

// VersionsList represents an object containing a list of paginated versions. This struct is based
// on the `pagination.page` struct which is returned when we call the `api.getVersions` endpoint
type VersionsList struct {
	Items      []models.Version `json:"items"`
	Count      int              `json:"count"`
	Offset     int              `json:"offset"`
	Limit      int              `json:"limit"`
	TotalCount int              `json:"total_count"`
}

// GetVersions gets all versions for an edition from the dataset api
func (c *Client) GetVersions(ctx context.Context, headers Headers, datasetID, editionID string, queryParams *QueryParams) (versionsList VersionsList, err error) {
	versionsList = VersionsList{}
	uri, err := url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions")
	if err != nil {
		return versionsList, err
	}

	req, err := http.NewRequest(http.MethodGet, uri, http.NoBody)
	if err != nil {
		return versionsList, err
	}

	// Add auth headers to the request
	headers.Add(req)

	// Add query params to request if valid
	if queryParams != nil {
		if err := queryParams.Validate(); err != nil {
			return versionsList, err
		}
		requestQuery := req.URL.Query()
		requestQuery.Add("limit", strconv.Itoa(queryParams.Limit))
		requestQuery.Add("offset", strconv.Itoa(queryParams.Offset))
		req.URL.RawQuery = requestQuery.Encode()
	}

	resp, err := c.hcCli.Client.Do(ctx, req)
	if err != nil {
		return versionsList, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshall the response body to target
	err = unmarshalResponseBody(resp, &versionsList)

	return versionsList, err
}
