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
	collectionID, datasetID, editionID, versionID string) (v models.Version, err error) {
	v = models.Version{}
	uri, err := url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", editionID, "versions", versionID)
	if err != nil {
		return v, err
	}

	req, err := http.NewRequest(http.MethodGet, uri, http.NoBody)
	if err != nil {
		return v, err
	}

	// Add auth headers
	addCollectionIDHeader(req, collectionID)
	dpNetRequest.AddFlorenceHeader(req, userAccessToken)
	dpNetRequest.AddServiceTokenHeader(req, serviceToken)
	dpNetRequest.AddDownloadServiceTokenHeader(req, downloadServiceToken)

	resp, err := c.hcCli.Client.Do(ctx, req)
	if err != nil {
		return v, err
	}

	defer closeResponseBody(ctx, resp)

	if resp.StatusCode != http.StatusOK {
		var errString string
		errResponseReadErr := json.NewDecoder(resp.Body).Decode(&errString)
		if errResponseReadErr != nil {
			errString = "Client failed to read DatasetAPI body"
		}
		err = errors.New(errString)
		return v, err
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return v, err
	}

	err = json.Unmarshal(b, &v)

	return v, err
}

// GetVersions gets all versions for an edition from the dataset api
// func (c *Client) GetVersions(ctx context.Context, userAccessToken, serviceToken, downloadServiceToken,
// 	collectionID, datasetID, editionID string, q *QueryParams) (m VersionsList, err error) {
// 	uri := fmt.Sprintf("%s/datasets/%s/editions/%s/versions", c.hcCli.URL, datasetID, editionID)
// 	if q != nil {
// 		if err = q.Validate(); err != nil {
// 			return
// 		}
// 		uri = fmt.Sprintf("%s?offset=%d&limit=%d", uri, q.Offset, q.Limit)
// 	}

// 	resp, err := c.doGetWithAuthHeadersAndWithDownloadToken(ctx, userAuthToken, serviceAuthToken, downloadServiceAuthToken, collectionID, uri)
// 	if err != nil {
// 		return
// 	}
// 	defer closeResponseBody(ctx, resp)

// 	if resp.StatusCode != http.StatusOK {
// 		err = NewDatasetAPIResponse(resp, uri)
// 		return
// 	}

// 	b, err := ioutil.ReadAll(resp.Body)
// 	if err != nil {
// 		return
// 	}

// 	if err = json.Unmarshal(b, &m); err != nil {
// 		return
// 	}

// 	return
// }
