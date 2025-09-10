package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/ONSdigital/dp-dataset-api/models"
)

const (
	maxIDs = 200
)

// QueryParams represents the possible query parameters that a caller can provide
type QueryParams struct {
	IDs       []string
	IsBasedOn string
	State     string
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
	// Build uri
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions", versionID)
	if err != nil {
		return version, err
	}

	// Make request
	resp, err := c.DoAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return version, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBodyExpectingErrorResponse(resp, &version)

	return version, err
}

// GetVersionV2 does the same as GetVersion but uses unmarshalResponseBodyExpectingErrorResponseV2
func (c *Client) GetVersionV2(ctx context.Context, headers Headers, datasetID, editionID, versionID string) (version models.Version, err error) {
	version = models.Version{}
	// Build uri
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions", versionID)
	if err != nil {
		return version, err
	}

	// Make request
	resp, err := c.DoAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return version, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBodyExpectingErrorResponseV2(resp, &version)

	return version, err
}

// VersionDimensionsList represent a list of Dimension
type VersionDimensionsList struct {
	Items []models.Dimension
}

// GetVersionDimensions will return a list of dimensions for a given version of a dataset
func (c *Client) GetVersionDimensions(ctx context.Context, headers Headers, datasetID, editionID, versionID string) (versionDimensionsList VersionDimensionsList, err error) {
	versionDimensionsList = VersionDimensionsList{}
	// Build uri
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions", versionID, "dimensions")
	if err != nil {
		return versionDimensionsList, err
	}

	// Make request
	resp, err := c.DoAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return versionDimensionsList, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBodyExpectingStringError(resp, &versionDimensionsList)

	return versionDimensionsList, err
}

// VersionDimensionOptionsList represent a list of PublicDimensionOption
type VersionDimensionOptionsList struct {
	Items []models.PublicDimensionOption
}

func (m VersionDimensionOptionsList) ToString() string {
	var b bytes.Buffer

	if len(m.Items) > 0 {
		b.WriteString(fmt.Sprintf("\n\tTitle: %s\n", m.Items[0].Name))
		var labels, options []string

		for i := range m.Items {
			dim := m.Items[i]
			labels = append(labels, dim.Label)
			options = append(options, dim.Option)
		}

		b.WriteString(fmt.Sprintf("\tLabels: %s\n", labels))
		b.WriteString(fmt.Sprintf("\tOptions: %v\n", options))
	}

	return b.String()
}

// Returns the options for a dimension
func (c *Client) GetVersionDimensionOptions(ctx context.Context, headers Headers, datasetID, editionID, versionID, dimensionID string, queryParams *QueryParams) (versionDimensionOptionsList VersionDimensionOptionsList, err error) {
	versionDimensionOptionsList = VersionDimensionOptionsList{}
	// Build uri
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions", versionID, "dimensions", dimensionID, "options")
	if err != nil {
		return versionDimensionOptionsList, err
	}

	// Add query params to request if valid
	if queryParams != nil {
		if err := queryParams.Validate(); err != nil {
			return versionDimensionOptionsList, err
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
		return versionDimensionOptionsList, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBodyExpectingStringError(resp, &versionDimensionOptionsList)

	return versionDimensionOptionsList, err
}

// GetVersionMetadata returns the metadata for a given dataset id, edition and version
func (c *Client) GetVersionMetadata(ctx context.Context, headers Headers, datasetID, editionID, versionID string) (metadata models.Metadata, err error) {
	metadata = models.Metadata{}
	// Build uri
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions", versionID, "metadata")
	if err != nil {
		return metadata, err
	}

	// Make request
	resp, err := c.DoAuthenticatedGetRequest(ctx, headers, uri)
	if err != nil {
		return metadata, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBodyExpectingStringError(resp, &metadata)

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
	// Build uri
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions")
	if err != nil {
		return versionsList, err
	}

	// Add query params to request if valid
	if queryParams != nil {
		if err := queryParams.Validate(); err != nil {
			return versionsList, err
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
		return versionsList, err
	}

	defer closeResponseBody(ctx, resp)

	// Unmarshal the response body to target
	err = unmarshalResponseBodyExpectingStringError(resp, &versionsList)

	return versionsList, err
}

func (c *Client) PutVersion(ctx context.Context, headers Headers, datasetID, editionID, versionID string, version models.Version) (updatedVersion models.Version, err error) {
	if err := validateRequiredParams(map[string]string{
		"datasetID": datasetID,
		"editionID": editionID,
		"versionID": versionID,
	}); err != nil {
		return updatedVersion, err
	}

	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions", versionID)
	if err != nil {
		return updatedVersion, err
	}

	requestBody, err := json.Marshal(version)
	if err != nil {
		return updatedVersion, err
	}

	resp, err := c.DoAuthenticatedPutRequest(ctx, headers, uri, requestBody)
	defer closeResponseBody(ctx, resp)

	if err != nil {
		return updatedVersion, err
	}

	if resp.StatusCode > 299 || resp.StatusCode < 200 {
		responseBody, err := getStringResponseBody(resp)
		if err != nil {
			return updatedVersion, fmt.Errorf("did not receive success response. received status %d", resp.StatusCode)
		}
		return updatedVersion, fmt.Errorf("did not receive success response. received status %d, response body: %s", resp.StatusCode, *responseBody)
	}

	err = json.NewDecoder(resp.Body).Decode(&updatedVersion)
	if err != nil {
		return updatedVersion, err
	}

	return updatedVersion, nil
}

func (c *Client) PutVersionState(ctx context.Context, headers Headers, datasetID, editionID, versionID, state string) (err error) {
	if err := validateRequiredParams(map[string]string{
		"datasetID": datasetID,
		"editionID": editionID,
		"versionID": versionID,
		"state":     state,
	}); err != nil {
		return err
	}

	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions", versionID, "state")
	if err != nil {
		return err
	}

	stateUpdate := models.StateUpdate{
		State: state,
	}

	requestBody, err := json.Marshal(stateUpdate)

	if err != nil {
		return err
	}

	resp, err := c.DoAuthenticatedPutRequest(ctx, headers, uri, requestBody)
	defer closeResponseBody(ctx, resp)

	if err != nil {
		return err
	}

	if resp.StatusCode > 299 || resp.StatusCode < 200 {
		responseBody, err := getStringResponseBody(resp)
		if err != nil {
			return fmt.Errorf("did not receive success response. received status %d", resp.StatusCode)
		}

		return fmt.Errorf("did not receive success response. received status %d, response body: %s", resp.StatusCode, *responseBody)
	}

	return nil
}

// Validate that all the specified params are not empty, and return an error message describing which ones are empty (if any)
func validateRequiredParams(params map[string]string) error {
	var missing []string
	for name, value := range params {
		if value == "" {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf("required args cannot be empty: %s", strings.Join(missing, ", "))
	}

	return nil
}
