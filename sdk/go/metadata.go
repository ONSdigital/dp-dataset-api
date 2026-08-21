package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/pkg/errors"
)

// PutMetadata updates the dataset and the version metadata
func (c *Client) PutMetadata(ctx context.Context, headers Headers, datasetID, edition, version string, metadata models.EditableMetadata) error {
	var err error
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "datasets", datasetID, "editions", edition, "versions", version, "metadata")
	if err != nil {
		return err
	}

	payload, err := json.Marshal(metadata)
	if err != nil {
		return errors.Wrap(err, "error while attempting to marshall metadata")
	}

	resp, err := c.doAuthenticatedPutRequest(ctx, headers, uri, payload)
	if err != nil {
		return err
	}
	defer closeResponseBody(ctx, resp)

	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusMultipleChoices {
		responseBody, err := getStringResponseBody(resp)
		if err != nil {
			return fmt.Errorf("did not receive success response. received status %d", resp.StatusCode)
		}
		return fmt.Errorf("did not receive success response. received status %d, response body: %s", resp.StatusCode, *responseBody)
	}

	return nil
}
