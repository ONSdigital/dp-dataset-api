package steps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-dataset-api/download"
	"github.com/ONSdigital/dp-dataset-api/schema"
	"github.com/google/go-cmp/cmp"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/cucumber/godog"
	"github.com/stretchr/testify/assert"

	"go.mongodb.org/mongo-driver/bson"

	assistdog "github.com/ONSdigital/dp-assistdog"
)

func WellKnownTestTime() time.Time {
	testTime, _ := time.Parse("2006-01-02T15:04:05Z", "2021-01-01T00:00:00Z")
	return testTime
}

func (c *DatasetComponent) RegisterSteps(ctx *godog.ScenarioContext) {
	c.apiFeature.RegisterSteps(ctx)

	ctx.Step(`^private endpoints are enabled$`, c.privateEndpointsAreEnabled)
	ctx.Step(`^URL rewriting is enabled$`, c.URLRewritingIsEnabled)
	ctx.Step(`^the document in the database for id "([^"]*)" should be:$`, c.theDocumentInTheDatabaseForIDShouldBe)
	ctx.Step(`^the instance in the database for id "([^"]*)" should be:$`, c.theInstanceInTheDatabaseForIDShouldBe)
	ctx.Step(`^the version in the database for id "([^"]*)" should be:$`, c.theVersionInTheDatabaseForIDShouldBe)
	ctx.Step(`^there are no datasets$`, c.thereAreNoDatasets)
	ctx.Step(`^I have these datasets:$`, c.iHaveTheseDatasets)
	ctx.Step(`^I have these "([^"]*)" datasets:$`, c.iHaveTheseConditionalDatasets)
	ctx.Step(`^I have these editions:$`, c.iHaveTheseEditions)
	ctx.Step(`^I have these versions:$`, c.iHaveTheseVersions)
	ctx.Step(`^I have these static versions:$`, c.iHaveTheseStaticVersions)
	ctx.Step(`^these versions need to be published:$`, c.theseVersionsNeedToBePublished)
	ctx.Step(`^I have these dimensions:$`, c.iHaveTheseDimensions)
	ctx.Step(`^I have these instances:$`, c.iHaveTheseInstances)
	ctx.Step(`^I have a real kafka container with topic "([^"]*)"$`, c.iHaveARealKafkaContainerWithTopic)
	ctx.Step(`^these cantabular generator downloads events are produced:$`, c.theseCantabularGeneratorDownloadsEventsAreProduced)
	ctx.Step(`^these generate downloads events are produced:$`, c.theseGenerateDownloadsEventsAreProduced)
	ctx.Step(`^I have a static dataset with version:$`, c.iHaveStaticDatasetWithVersion)
	ctx.Step(`^the dataset "([^"]*)" should exist$`, c.datasetShouldExist)
	ctx.Step(`^the dataset "([^"]*)" should not exist$`, c.datasetShouldNotExist)
	ctx.Step(`^the static version "([^"]*)" should exist$`, c.staticVersionShouldExist)
	ctx.Step(`^the static version "([^"]*)" should not exist$`, c.staticVersionShouldNotExist)
	ctx.Step(`^the response header "([^"]*)" should not be empty$`, c.theResponseHeaderShouldNotBeEmpty)
	ctx.Step(`^the dataset "([^"]*)" should have next equal to current$`, c.theDatasetShouldHaveNextEqualToCurrent)
	ctx.Step(`^the "([^"]*)" feature flag is "([^"]*)"$`, c.theFeatureFlagIs)
	ctx.Step(`^cloudflare cache purge should have been called for dataset "([^"]*)" and edition "([^"]*)"$`, c.cloudflareCachePurgeShouldHaveBeenCalled)
	ctx.Step(`^cloudflare cache purge should not have been called$`, c.cloudflareCachePurgeShouldNotHaveBeenCalled)
	ctx.Step(`^cloudflare cache purge is configured to fail$`, c.cloudflareCachePurgeIsConfiguredToFail)
}

func (c *DatasetComponent) theFeatureFlagIs(flagName, status string) error {
	switch flagName {
	case "ENABLE_DETACH_DATASET":
		c.Config.EnableDetachDataset = status == "true"
	case "ENABLE_DELETE_STATIC_VERSION":
		c.Config.EnableDeleteStaticVersion = status == "true"
	default:
		return fmt.Errorf("unknown feature flag: %s", flagName)
	}
	return nil
}

func (c *DatasetComponent) thereAreNoDatasets() error {
	return c.MongoClient.Connection.DropDatabase(context.Background())
}

func (c *DatasetComponent) privateEndpointsAreEnabled() error {
	c.Config.EnablePrivateEndpoints = true
	return nil
}

func (c *DatasetComponent) URLRewritingIsEnabled() error {
	c.Config.EnableURLRewriting = true
	return nil
}

func (c *DatasetComponent) theDocumentInTheDatabaseForIDShouldBe(documentID string, documentJSON *godog.DocString) error {
	var expectedDataset models.Dataset

	if err := json.Unmarshal([]byte(documentJSON.Content), &expectedDataset); err != nil {
		return err
	}

	collectionName := c.MongoClient.ActualCollectionName(config.DatasetsCollection)
	var link models.DatasetUpdate
	if err := c.MongoClient.Connection.Collection(collectionName).FindOne(context.Background(), bson.M{"_id": documentID}, &link); err != nil {
		return err
	}

	assert.Equal(&c.ErrorFeature, documentID, link.ID)

	document := link.Next

	// Remove the last updated value so to be able to compare the datasets
	// otherwise the assertion would always fail as last updated would be "now"
	document.LastUpdated = time.Time{}

	assert.Equal(&c.ErrorFeature, expectedDataset, *document)

	return c.ErrorFeature.StepError()
}

func (c *DatasetComponent) theInstanceInTheDatabaseForIDShouldBe(id string, body *godog.DocString) error {
	var expected models.Instance

	if err := json.Unmarshal([]byte(body.Content), &expected); err != nil {
		return fmt.Errorf("failed to unmarshal body: %w", err)
	}

	collectionName := c.MongoClient.ActualCollectionName(config.InstanceCollection)
	var got models.Instance

	if err := c.MongoClient.Connection.Collection(collectionName).FindOne(context.Background(), bson.M{"_id": id}, &got); err != nil {
		return fmt.Errorf("failed to get instance from collection: %w", err)
	}

	assert.Equal(&c.ErrorFeature, expected, got)

	return nil
}

func (c *DatasetComponent) theVersionInTheDatabaseForIDShouldBe(id string, body *godog.DocString) error {
	var expected models.Version

	if err := json.Unmarshal([]byte(body.Content), &expected); err != nil {
		return fmt.Errorf("failed to unmarshal body: %w", err)
	}

	collectionName := c.MongoClient.ActualCollectionName(config.InstanceCollection)
	var got models.Version

	if err := c.MongoClient.Connection.Collection(collectionName).FindOne(context.Background(), bson.M{"_id": id}, &got); err != nil {
		return fmt.Errorf("failed to get version from collection: %w", err)
	}

	// Remove the last updated value so to be able to compare the datasets
	// otherwise the assertion would always fail as last updated would be "now"
	got.LastUpdated = time.Time{}
	got.Links.Version = nil // This can't be checked (json omitted)

	if expected.ETag == "" {
		// Ignore generated etag if we are not concerned about it
		expected.ETag = got.ETag
	}

	assert.Equal(&c.ErrorFeature, expected, got)

	return c.ErrorFeature.StepError()
}

func (c *DatasetComponent) iHaveARealKafkaContainerWithTopic(topic string) error {
	err := c.setConsumer(topic)
	if err != nil {
		return fmt.Errorf("failed to create kafka consumer: %w", err)
	}
	c.setInitialiserRealKafka()

	return nil
}

// theseCsvCreatedEventsAreProduced consumes kafka messages that are expected to be produced by the service under test
// and validates that they match the expected values in the test
func (c *DatasetComponent) theseGenerateDownloadsEventsAreProduced(events *godog.Table) error {
	expected, err := assistdog.NewDefault().CreateSlice(new(download.GenerateDownloads), events)
	if err != nil {
		return fmt.Errorf("failed to create slice from godog table: %w", err)
	}

	var got []*download.GenerateDownloads
	listen := true

	for listen {
		select {
		// ToDo: Set timeout variable

		case <-time.After(time.Second * 15):
			listen = false
		case <-c.consumer.Channels().Closer:
			return errors.New("closer channel closed")
		case msg, ok := <-c.consumer.Channels().Upstream:
			if !ok {
				return errors.New("upstream channel closed")
			}

			var e download.GenerateDownloads
			var s = schema.GenerateCMDDownloadsEvent

			if err := s.Unmarshal(msg.GetData(), &e); err != nil {
				msg.Commit()
				msg.Release()
				return fmt.Errorf("error unmarshalling message: %w", err)
			}

			msg.Commit()
			msg.Release()

			got = append(got, &e)
		}
	}
	if diff := cmp.Diff(got, expected); diff != "" {
		return fmt.Errorf("-got +expected)\n%s", diff)
	}

	return nil
}

// we are passing the string array as [xxxx,yyyy,zzz]
// this is required to support array being used in kafka messages
func arrayParser(raw string) (interface{}, error) {
	// remove the starting and trailing brackets
	str := strings.Trim(raw, "[]")
	if str == "" {
		return []string{}, nil
	}

	strArray := strings.Split(str, ",")
	return strArray, nil
}

// theseCsvCreatedEventsAreProduced consumes kafka messages that are expected to be produced by the service under test
// and validates that they match the expected values in the test
func (c *DatasetComponent) theseCantabularGeneratorDownloadsEventsAreProduced(events *godog.Table) error {
	assist := assistdog.NewDefault()
	assist.RegisterParser([]string{}, arrayParser)
	expected, err := assist.CreateSlice(new(download.CantabularGeneratorDownloads), events)
	if err != nil {
		return fmt.Errorf("failed to create slice from godog table: %w", err)
	}

	var got []*download.CantabularGeneratorDownloads
	listen := true

	for listen {
		select {
		// ToDo: Set timeout variable

		case <-time.After(time.Second * 15):
			listen = false
		case <-c.consumer.Channels().Closer:
			return errors.New("closer channel closed")
		case msg, ok := <-c.consumer.Channels().Upstream:
			if !ok {
				return errors.New("upstream channel closed")
			}

			var e download.CantabularGeneratorDownloads
			var s = schema.GenerateCantabularDownloadsEvent

			if err := s.Unmarshal(msg.GetData(), &e); err != nil {
				msg.Commit()
				msg.Release()
				return fmt.Errorf("error unmarshalling message: %w", err)
			}

			msg.Commit()
			msg.Release()

			got = append(got, &e)
		}
	}
	if diff := cmp.Diff(got, expected); diff != "" {
		return fmt.Errorf("-got +expected)\n%s", diff)
	}

	return nil
}

func (c *DatasetComponent) iHaveTheseEditions(editionsJSON *godog.DocString) error {
	editions := []models.Edition{}

	err := json.Unmarshal([]byte(editionsJSON.Content), &editions)
	if err != nil {
		return err
	}

	for timeOffset := range editions {
		editionDoc := &editions[timeOffset]
		editionID := editionDoc.ID

		editionUp := models.EditionUpdate{
			ID:      editionID,
			Next:    editionDoc,
			Current: editionDoc,
		}

		editionsCollection := c.MongoClient.ActualCollectionName(config.EditionsCollection)
		err = c.putDocumentInDatabase(editionUp, editionID, editionsCollection, timeOffset)

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *DatasetComponent) iHaveTheseDatasets(datasetsJSON *godog.DocString) error {
	datasets := []models.Dataset{}

	err := json.Unmarshal([]byte(datasetsJSON.Content), &datasets)
	if err != nil {
		return err
	}

	for timeOffset := range datasets {
		datasetDoc := &datasets[timeOffset]
		datasetID := datasetDoc.ID
		datasetUp := models.DatasetUpdate{
			ID:      datasetID,
			Next:    datasetDoc,
			Current: datasetDoc,
		}

		datasetsCollection := c.MongoClient.ActualCollectionName(config.DatasetsCollection)
		if err := c.putDocumentInDatabase(datasetUp, datasetID, datasetsCollection, timeOffset); err != nil {
			return err
		}
	}

	return nil
}

// Done for GET /datasets?is_based_on so that we can condition a dataset on whether it is published or not.
func (c *DatasetComponent) iHaveTheseConditionalDatasets(status string, datasetsJSON *godog.DocString) error {
	datasets := []models.Dataset{}

	err := json.Unmarshal([]byte(datasetsJSON.Content), &datasets)
	if err != nil {
		return fmt.Errorf("failed to unmarshal: %w", err)
	}

	for timeOffset := range datasets {
		datasetDoc := &datasets[timeOffset]

		datasetID := datasetDoc.ID
		datasetUp := models.DatasetUpdate{
			ID: datasetID,
		}

		datasetUp.Current = datasetDoc
		if status == "private" {
			datasetUp.Next = datasetDoc
		}

		datasetsCollection := c.MongoClient.ActualCollectionName(config.DatasetsCollection)
		if err := c.putDocumentInDatabase(datasetUp, datasetID, datasetsCollection, timeOffset); err != nil {
			return fmt.Errorf("failed to insert to mongo: %w", err)
		}
	}

	return nil
}

func (c *DatasetComponent) iHaveTheseVersions(versionsJSON *godog.DocString) error {
	versions := []models.Version{}

	err := json.Unmarshal([]byte(versionsJSON.Content), &versions)
	if err != nil {
		return err
	}

	for timeOffset := range versions {
		version := &versions[timeOffset]
		versionID := version.ID
		// Some tests need to specify the version links document
		if version.Links.Version == nil {
			version.Links.Version = &models.LinkObject{
				HRef: version.Links.Self.HRef,
			}
		}
		// Set the etag (json omitted)
		version.ETag = "etag-" + version.ID

		instanceCollection := c.MongoClient.ActualCollectionName(config.InstanceCollection)
		if err := c.putDocumentInDatabase(version, versionID, instanceCollection, timeOffset); err != nil {
			return err
		}
	}

	return nil
}

func (c *DatasetComponent) iHaveTheseStaticVersions(versionsJSON *godog.DocString) error {
	versions := []models.Version{}

	err := json.Unmarshal([]byte(versionsJSON.Content), &versions)
	if err != nil {
		return fmt.Errorf("failed to unmarshal versionsJSON: %w", err)
	}

	versionsCollection := c.MongoClient.ActualCollectionName(config.VersionsCollection)

	for timeOffset := range versions {
		version := &versions[timeOffset]
		if version.Links.Version == nil {
			version.Links.Version = &models.LinkObject{
				HRef: version.Links.Edition.HRef + "/versions/" + strconv.Itoa(version.Version),
				ID:   strconv.Itoa(version.Version),
			}
		}
		if err := c.putDocumentInDatabase(version, version.ID, versionsCollection, timeOffset); err != nil {
			return fmt.Errorf("failed to insert static version: %w", err)
		}
	}

	return nil
}

func (c *DatasetComponent) theseVersionsNeedToBePublished(idsJSON *godog.DocString) error {
	var versions []struct {
		VersionID     string `json:"version_id"`
		VersionNumber string `json:"version_number"`
	}

	err := json.Unmarshal([]byte(idsJSON.Content), &versions)
	if err != nil {
		return fmt.Errorf("failed to unmarshal idsJSON: %w", err)
	}

	for i, v := range versions {
		verDoc := make(bson.M)
		verDoc["links.version.id"] = v.VersionNumber

		instanceCollection := c.MongoClient.ActualCollectionName(config.InstanceCollection)
		if err := c.updateDocumentInDatabase(verDoc, v.VersionID, instanceCollection, i); err != nil {
			return fmt.Errorf("failed to update database: %w", err)
		}
	}

	return nil
}

func (c *DatasetComponent) iHaveTheseDimensions(dimensionsJSON *godog.DocString) error {
	dimensions := []models.DimensionOption{}

	err := json.Unmarshal([]byte(dimensionsJSON.Content), &dimensions)
	if err != nil {
		return fmt.Errorf("failed to unmarshal dimensionsJSON: %w", err)
	}

	for timeOffset := range dimensions {
		dimension := &dimensions[timeOffset]
		dimensionID := dimension.Option

		dimensionOptionsCollection := c.MongoClient.ActualCollectionName(config.DimensionOptionsCollection)
		if err := c.putDocumentInDatabase(dimension, dimensionID, dimensionOptionsCollection, timeOffset); err != nil {
			return err
		}
	}

	return nil
}

func (c *DatasetComponent) iHaveTheseInstances(instancesJSON *godog.DocString) error {
	instances := []models.Instance{}

	err := json.Unmarshal([]byte(instancesJSON.Content), &instances)
	if err != nil {
		return err
	}

	for timeOffset := range instances {
		instance := &instances[timeOffset]
		instanceID := instance.InstanceID

		instanceCollection := c.MongoClient.ActualCollectionName(config.InstanceCollection)
		if err := c.putDocumentInDatabase(instance, instanceID, instanceCollection, timeOffset); err != nil {
			return err
		}
	}

	return nil
}

func (c *DatasetComponent) updateDocumentInDatabase(document bson.M, id, collectionName string, _ int) error {
	update := bson.M{
		"$set": document,
	}

	_, err := c.MongoClient.Connection.Collection(collectionName).UpdateById(context.Background(), id, update)
	if err != nil {
		return fmt.Errorf("failed to update document in DB: %w", err)
	}
	return nil
}

func (c *DatasetComponent) putDocumentInDatabase(document interface{}, id, collectionName string, timeOffset int) error {
	update := bson.M{
		"$set": document,
		"$setOnInsert": bson.M{
			"last_updated": WellKnownTestTime().Add(time.Second * time.Duration(timeOffset)),
		},
	}

	_, err := c.MongoClient.Connection.Collection(collectionName).UpsertById(context.Background(), id, update)

	if err != nil {
		return err
	}
	return nil
}

func (c *DatasetComponent) iHaveStaticDatasetWithVersion(jsonData *godog.DocString) error {
	var data struct {
		Dataset models.Dataset `json:"dataset"`
		Version models.Version `json:"version"`
	}
	if err := json.Unmarshal([]byte(jsonData.Content), &data); err != nil {
		return fmt.Errorf("failed to unmarshal static dataset data: %w", err)
	}
	datasetID := data.Dataset.ID
	data.Dataset.Type = "static"
	datasetUp := models.DatasetUpdate{
		ID:      datasetID,
		Next:    &data.Dataset,
		Current: &data.Dataset,
	}
	datasetsCollection := c.MongoClient.ActualCollectionName(config.DatasetsCollection)
	if err := c.putDocumentInDatabase(datasetUp, datasetID, datasetsCollection, 0); err != nil {
		return fmt.Errorf("failed to insert static dataset: %w", err)
	}
	versionID := data.Version.ID

	if data.Version.Links == nil {
		data.Version.Links = &models.VersionLinks{}
	}

	if data.Version.Links.Self == nil {
		data.Version.Links.Self = &models.LinkObject{
			HRef: fmt.Sprintf("/datasets/%s/editions/%s/versions/%d", datasetID, data.Version.Edition, data.Version.Version),
		}
	}

	if data.Version.Links.Version == nil {
		data.Version.Links.Version = &models.LinkObject{
			HRef: data.Version.Links.Self.HRef,
			ID:   fmt.Sprintf("%d", data.Version.Version),
		}
	}

	data.Version.ETag = "etag-" + versionID
	versionsCollection := c.MongoClient.ActualCollectionName(config.VersionsCollection)
	if err := c.putDocumentInDatabase(data.Version, versionID, versionsCollection, 0); err != nil {
		return fmt.Errorf("failed to insert static version: %w", err)
	}
	return nil
}

// checkDocumentExistence checks for the existence of a document by ID in a given collection.
func (c *DatasetComponent) checkDocumentExistence(collectionName, id string, shouldExist bool) error {
	collection := c.MongoClient.ActualCollectionName(collectionName)
	var result map[string]interface{}

	err := c.MongoClient.Connection.Collection(collection).FindOne(context.Background(), bson.M{"_id": id}, &result)

	if shouldExist {
		if err != nil {
			if strings.Contains(err.Error(), "no documents in result") {
				return fmt.Errorf("expected document with ID '%s' in collection '%s' but it was not found: %w", id, collection, err)
			}
			return fmt.Errorf("failed to check for existence of document with ID '%s' in collection '%s': %w", id, collection, err)
		}
		return nil
	}

	if err == nil {
		return fmt.Errorf("did not expect document with ID '%s' in collection '%s' but it was found", id, collection)
	}

	if strings.Contains(err.Error(), "no documents in result") {
		return nil
	}

	return fmt.Errorf("unexpected error when checking non-existence of document with ID '%s' in collection '%s': %w", id, collection, err)
}

// datasetShouldExist checks for the existence of a dataset document in the datasets collection
func (c *DatasetComponent) datasetShouldExist(datasetID string) error {
	return c.checkDocumentExistence(config.DatasetsCollection, datasetID, true)
}

// datasetShouldNotExist checks the dataset does not exist in the datasets collection
func (c *DatasetComponent) datasetShouldNotExist(datasetID string) error {
	return c.checkDocumentExistence(config.DatasetsCollection, datasetID, false)
}

// staticVersionShouldExist checks for the existence of a version document in the versions collection
func (c *DatasetComponent) staticVersionShouldExist(versionID string) error {
	return c.checkDocumentExistence(config.VersionsCollection, versionID, true)
}

// staticVersionShouldNotExist checks the version document does not exist in the versions collection
func (c *DatasetComponent) staticVersionShouldNotExist(versionID string) error {
	return c.checkDocumentExistence(config.VersionsCollection, versionID, false)
}

func (c *DatasetComponent) theDatasetShouldHaveNextEqualToCurrent(datasetID string) error {
	collectionName := c.MongoClient.ActualCollectionName(config.DatasetsCollection)
	var dataset models.DatasetUpdate

	if err := c.MongoClient.Connection.Collection(collectionName).FindOne(context.Background(), bson.M{"_id": datasetID}, &dataset); err != nil {
		return err
	}

	if dataset.Next == nil || dataset.Current == nil {
		return fmt.Errorf("dataset %s has nil next or current document", datasetID)
	}

	next := *dataset.Next
	current := *dataset.Current

	if diff := cmp.Diff(next, current); diff != "" {
		return fmt.Errorf("next and current do not match for dataset %s:\n%s", datasetID, diff)
	}
	return nil
}

func (c *DatasetComponent) theResponseHeaderShouldNotBeEmpty(header string) error {
	value := c.apiFeature.HTTPResponse.Header.Get(header)
	if value == "" {
		return fmt.Errorf("expected non-empty header %q but got empty", header)
	}
	return nil
}

func (c *DatasetComponent) cloudflareCachePurgeShouldHaveBeenCalled(datasetID, editionID string) error {
	for _, call := range c.CloudflarePurgeCalls {
		if call.DatasetID == datasetID && call.EditionID == editionID {
			return nil
		}
	}

	return fmt.Errorf("expected cloudflare cache purge to be called for dataset '%s' and edition '%s', but it was not. Calls made: %+v",
		datasetID, editionID, c.CloudflarePurgeCalls)
}

func (c *DatasetComponent) cloudflareCachePurgeShouldNotHaveBeenCalled() error {
	if len(c.CloudflarePurgeCalls) > 0 {
		return fmt.Errorf("expected no cloudflare cache purge calls, but got %d calls: %+v",
			len(c.CloudflarePurgeCalls), c.CloudflarePurgeCalls)
	}
	return nil
}

func (c *DatasetComponent) cloudflareCachePurgeIsConfiguredToFail() error {
	c.MockCloudflare.ShouldFail = true
	return nil
}
