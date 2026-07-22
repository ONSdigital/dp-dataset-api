package application

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ONSdigital/dp-api-clients-go/headers"
	"github.com/ONSdigital/dp-dataset-api/models"
	topicAPISDK "github.com/ONSdigital/dp-topic-api/sdk"
)

// EnsureVersionWebPageLink ensures the topic slug used in the version web page link matches the topic slug that stored in the topic API.
// If the topic slug is different, it will update the version web page link to use the topic slug from the topic API.
// Returns the updated version or the original version if no update was needed and a boolean indicating whether an update was made.
func (smDS *StateMachineDatasetAPI) EnsureVersionWebPageLink(ctx context.Context, datasetID string, version *models.Version, accessToken string) (*models.Version, bool, error) {
	if version == nil {
		return nil, false, errors.New("version is nil")
	}

	if version.Links == nil || version.Links.WebPage == nil || version.Links.WebPage.HRef == "" {
		return version, false, errors.New("version is missing webpage link")
	}

	datasetUpdate, err := smDS.DataStore.Backend.GetDataset(ctx, datasetID)
	if err != nil {
		return version, false, fmt.Errorf("failed to get dataset from datastore: %w", err)
	}

	if datasetUpdate == nil || datasetUpdate.Next == nil || len(datasetUpdate.Next.Topics) == 0 {
		return version, false, errors.New("dataset is missing topics")
	}

	topicResp, err := smDS.TopicAPIClient.GetTopicPrivate(ctx, topicAPISDK.Headers{ServiceAuthToken: accessToken}, datasetUpdate.Next.Topics[0])
	if err != nil {
		return version, false, fmt.Errorf("failed to get topic from topic API: %w", err)
	}

	if topicResp == nil || topicResp.Current == nil || topicResp.Current.Slug == "" {
		return version, false, errors.New("topic response is missing slug")
	}

	topicSlug := topicResp.Current.Slug

	trimmedWebPageLink := strings.Trim(version.Links.WebPage.HRef, "/")
	storedTopicSlug := strings.Split(trimmedWebPageLink, "/")[0]

	// If the topic slug hasn't changed then return early with no changes to the version.
	if storedTopicSlug == topicSlug {
		return version, false, nil
	}

	// Topic slug has changed so version needs to be updated with the new topic slug in the web page link.
	updatedVersion := *version
	updatedVersion.Links = version.Links.DeepCopy()
	updatedVersion.Links.WebPage.HRef = fmt.Sprintf("/%s/datasets/%s/editions/%s/versions/%d", topicSlug, datasetID, version.Edition, version.Version)

	eTag := headers.IfMatchAnyETag
	if version.ETag != "" {
		eTag = version.ETag
	}

	newETag, err := smDS.DataStore.Backend.UpdateVersionStatic(ctx, version, &updatedVersion, eTag)
	if err != nil {
		return version, false, fmt.Errorf("failed to update version with new webpage link: %w", err)
	}
	updatedVersion.ETag = newETag

	// Version needs to be re-fetched from database as fields may have been updated on update (e.g. last_updated).
	latestStoredVersion, err := smDS.DataStore.Backend.GetVersionStatic(ctx, datasetID, version.Edition, version.Version, "")
	if err != nil {
		return &updatedVersion, true, fmt.Errorf("failed to get latest version after update: %w", err)
	}

	return latestStoredVersion, true, nil
}
