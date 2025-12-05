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

// EditableMetadata represents the metadata fields that can be edited
type EditableMetadata struct {
	Alerts            *[]models.Alert         `json:"alerts,omitempty"`
	CanonicalTopic    string                  `json:"canonical_topic,omitempty"`
	Contacts          []models.ContactDetails `json:"contacts,omitempty"`
	Description       string                  `json:"description,omitempty"`
	Dimensions        []models.Dimension      `json:"dimensions,omitempty"`
	Keywords          []string                `json:"keywords,omitempty"`
	LatestChanges     *[]models.LatestChange  `json:"latest_changes,omitempty"`
	License           string                  `json:"license,omitempty"`
	Methodologies     []models.GeneralDetails `json:"methodologies,omitempty"`
	NationalStatistic *bool                   `json:"national_statistic,omitempty"`
	NextRelease       string                  `json:"next_release,omitempty"`
	Publications      []models.GeneralDetails `json:"publications,omitempty"`
	QMI               *models.GeneralDetails  `json:"qmi,omitempty"`
	RelatedDatasets   []models.GeneralDetails `json:"related_datasets,omitempty"`
	ReleaseDate       string                  `json:"release_date,omitempty"`
	ReleaseFrequency  string                  `json:"release_frequency,omitempty"`
	Title             string                  `json:"title,omitempty"`
	Survey            string                  `json:"survey,omitempty"`
	Subtopics         []string                `json:"subtopics,omitempty"`
	UnitOfMeasure     string                  `json:"unit_of_measure,omitempty"`
	UsageNotes        *[]models.UsageNote     `json:"usage_notes,omitempty"`
	RelatedContent    []models.GeneralDetails `json:"related_content,omitempty"`
}

// PutMetadata updates the dataset and the version metadata
func (c *Client) PutMetadata(ctx context.Context, headers Headers, datasetID, edition, version string, metadata EditableMetadata, versionEtag string) error {
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

	resp, err := c.DoAuthenticatedPutRequestWithEtag(ctx, headers, uri, payload, versionEtag)
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
