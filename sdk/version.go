package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/ONSdigital/dp-dataset-api/models"
	dpNetRequest "github.com/ONSdigital/dp-net/v2/request"
)

// GetVersion gets a specific version for an edition from the dataset api
func (c *Client) GetVersion(ctx context.Context, userAccessToken, serviceToken,
	downloadServiceToken, collectionID, datasetID, editionID, versionID string) (v models.Version, err error) {
	v = models.Version{}

	uri := fmt.Sprintf("%s/datasets/%s/editions/%s/versions/%s", c.hcCli.URL, datasetID, editionID, versionID)

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
