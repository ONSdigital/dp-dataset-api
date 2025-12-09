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

type UpdateInstance struct {
	Alerts               *[]models.Alert             `json:"alerts"`
	CollectionID         string                      `json:"collection_id"`
	Downloads            models.DownloadList         `json:"downloads"`
	Edition              string                      `json:"edition"`
	Dimensions           []models.Dimension          `json:"dimensions"`
	ID                   string                      `json:"id"`
	InstanceID           string                      `json:"instance_id"`
	LatestChanges        []models.LatestChange       `json:"latest_changes"`
	ReleaseDate          string                      `json:"release_date"`
	State                string                      `json:"state"`
	Temporal             []models.TemporalFrequency  `json:"temporal"`
	Version              int                         `json:"version"`
	NumberOfObservations int64                       `json:"total_observations,omitempty"`
	ImportTasks          *models.InstanceImportTasks `json:"import_tasks,omitempty"`
	CSVHeader            []string                    `json:"headers,omitempty"`
	Type                 string                      `json:"type,omitempty"`
	IsBasedOn            *models.IsBasedOn           `json:"is_based_on,omitempty"`
}

// PutInstance updates an instance
func (c *Client) PutInstance(ctx context.Context, headers Headers, instanceID string, i UpdateInstance, ifMatch string) (eTag string, err error) {
	uri := &url.URL{}
	uri.Path, err = url.JoinPath(c.hcCli.URL, "instances", instanceID)
	if err != nil {
		return "", err
	}

	payload, err := json.Marshal(i)
	if err != nil {
		return "", errors.Wrap(err, "error while attempting to marshall instance")
	}

	resp, err := c.DoAuthenticatedPutRequestWithEtag(ctx, headers, uri, payload, ifMatch)
	if err != nil {
		return "", errors.Wrap(err, "http client returned error while attempting to make request")
	}
	defer closeResponseBody(ctx, resp)

	if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusMultipleChoices {
		responseBody, err := getStringResponseBody(resp)
		if err != nil {
			return "", fmt.Errorf("did not receive success response. received status %d", resp.StatusCode)
		}
		return "", fmt.Errorf("did not receive success response. received status %d, response body: %s", resp.StatusCode, *responseBody)
	}

	eTag, err = getResponseETag(resp)
	if err != nil && err != ErrHeaderNotFound {
		return "", err
	}

	return eTag, nil
}
