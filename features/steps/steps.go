package steps

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ONSdigital/dp-dataset-api/download"
	"github.com/ONSdigital/dp-dataset-api/schema"
	"github.com/google/go-cmp/cmp"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/cucumber/godog"
	"github.com/stretchr/testify/assert"

	"go.mongodb.org/mongo-driver/bson"

	assistdog "github.com/ONSdigital/dp-assistdog"
)

var WellKnownTestTime time.Time

func init() {
	WellKnownTestTime, _ = time.Parse("2006-01-02T15:04:05Z", "2021-01-01T00:00:00Z")
}

func (c *DatasetComponent) RegisterSteps(ctx *godog.ScenarioContext) {
	ctx.Step(`^private endpoints are enabled$`, c.privateEndpointsAreEnabled)
	ctx.Step(`^the document in the database for id "([^"]*)" should be:$`, c.theDocumentInTheDatabaseForIdShouldBe)
	ctx.Step(`^there are no datasets$`, c.thereAreNoDatasets)
	ctx.Step(`^I have these datasets:$`, c.iHaveTheseDatasets)
	ctx.Step(`^I have these editions:$`, c.iHaveTheseEditions)
	ctx.Step(`^I have these versions:$`, c.iHaveTheseVersions)
	ctx.Step(`^these versions need to be published:$`, c.theseVersionsNeedToBePublished)
	ctx.Step(`^I have these dimensions:$`, c.iHaveTheseDimensions)
	ctx.Step(`^I have these instances:$`, c.iHaveTheseInstances)
	ctx.Step(`^I have a real kafka container with topic "([^"]*)"$`, c.iHaveARealKafkaContainerWithTopic)
	ctx.Step(`^these cantabular generator downloads events are produced:$`, c.theseCantabularGeneratorDownloadsEventsAreProduced)
	ctx.Step(`^these generate downloads events are produced:$`, c.theseGenerateDownloadsEventsAreProduced)
	ctx.Step(`^I access the root census endpoint$`, c.iAccessTheCensusEndpoint)
	ctx.Step(`^a list of named cantabular blobs is returned$`, c.aListOfNamedCantabularBlobsIsReturned)
	ctx.Step(`^I have some cantabular blobs$`, c.iHaveSomeCantabularBlobs)
}

func (c *DatasetComponent) thereAreNoDatasets() error {
	return c.MongoClient.Connection.DropDatabase(context.Background())

}

func (c *DatasetComponent) privateEndpointsAreEnabled() error {
	c.Config.EnablePrivateEndpoints = true
	return nil
}

func (c *DatasetComponent) theDocumentInTheDatabaseForIdShouldBe(documentId string, documentJson *godog.DocString) error {
	var expectedDataset models.Dataset

	if err := json.Unmarshal([]byte(documentJson.Content), &expectedDataset); err != nil {
		return err
	}

	var link models.DatasetUpdate
	if err := c.MongoClient.Connection.Collection("datasets").FindOne(context.Background(), bson.M{"_id": documentId}, &link); err != nil {
		return err
	}

	assert.Equal(&c.ErrorFeature, documentId, link.ID)

	document := link.Next

	assert.Equal(&c.ErrorFeature, expectedDataset.Title, document.Title)
	assert.Equal(&c.ErrorFeature, expectedDataset.State, document.State)

	return c.ErrorFeature.StepError()
}

func (c *DatasetComponent) iHaveARealKafkaContainerWithTopic(topic string) error {
	c.setConsumer(topic)
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
		return fmt.Errorf("-got +expected)\n%s\n", diff)
	}

	return nil
}

// theseCsvCreatedEventsAreProduced consumes kafka messages that are expected to be produced by the service under test
// and validates that they match the expected values in the test
func (c *DatasetComponent) theseCantabularGeneratorDownloadsEventsAreProduced(events *godog.Table) error {
	expected, err := assistdog.NewDefault().CreateSlice(new(download.CantabularGeneratorDownloads), events)
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
		return fmt.Errorf("-got +expected)\n%s\n", diff)
	}

	return nil
}

func (c *DatasetComponent) iHaveTheseEditions(editionsJson *godog.DocString) error {
	editions := []models.Edition{}

	err := json.Unmarshal([]byte(editionsJson.Content), &editions)
	if err != nil {
		return err
	}

	for timeOffset, editionDoc := range editions {
		editionID := editionDoc.ID

		editionUp := models.EditionUpdate{
			ID:      editionID,
			Next:    &editions[timeOffset],
			Current: &editions[timeOffset],
		}

		err = c.putDocumentInDatabase(editionUp, editionID, "editions", timeOffset)

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *DatasetComponent) iHaveTheseDatasets(datasetsJson *godog.DocString) error {

	datasets := []models.Dataset{}

	err := json.Unmarshal([]byte(datasetsJson.Content), &datasets)
	if err != nil {
		return err
	}

	for timeOffset, datasetDoc := range datasets {
		datasetID := datasetDoc.ID

		datasetUp := models.DatasetUpdate{
			ID:      datasetID,
			Next:    &datasets[timeOffset],
			Current: &datasets[timeOffset],
		}

		if err := c.putDocumentInDatabase(datasetUp, datasetID, "datasets", timeOffset); err != nil {
			return err
		}
	}

	return nil
}

func (c *DatasetComponent) iHaveTheseVersions(versionsJson *godog.DocString) error {
	versions := []models.Version{}

	err := json.Unmarshal([]byte(versionsJson.Content), &versions)
	if err != nil {
		return err
	}

	for timeOffset, version := range versions {
		versionID := version.ID
		// Some tests need to specify the version links document
		if version.Links.Version == nil {
			version.Links.Version = &models.LinkObject{
				HRef: version.Links.Self.HRef,
			}
		}

		if err := c.putDocumentInDatabase(version, versionID, "instances", timeOffset); err != nil {
			return err
		}
	}

	return nil
}

func (c *DatasetComponent) theseVersionsNeedToBePublished(idsJSON *godog.DocString) error {
	var versions []struct {
		VersionId     string `json:"version_id"`
		VersionNumber string `json:"version_number"`
	}

	err := json.Unmarshal([]byte(idsJSON.Content), &versions)
	if err != nil {
		return fmt.Errorf("failed to unmarshal idsJSON: %w", err)
	}

	for i, v := range versions {
		verDoc := make(bson.M)
		verDoc["links.version.id"] = v.VersionNumber

		if err := c.updateDocumentInDatabase(verDoc, v.VersionId, "instances", i); err != nil {
			return fmt.Errorf("failed to update database: %w", err)
		}
	}

	return nil
}

func (c *DatasetComponent) iHaveTheseDimensions(dimensionsJson *godog.DocString) error {
	dimensions := []models.DimensionOption{}

	err := json.Unmarshal([]byte(dimensionsJson.Content), &dimensions)
	if err != nil {
		return fmt.Errorf("failed to unmarshal dimensionsJson: %w", err)
	}

	for timeOffset, dimension := range dimensions {
		dimensionID := dimension.Option

		if err := c.putDocumentInDatabase(dimension, dimensionID, "dimension.options", timeOffset); err != nil {
			return err
		}
	}

	return nil
}

func (c *DatasetComponent) iHaveTheseInstances(instancesJson *godog.DocString) error {
	instances := []models.Instance{}

	err := json.Unmarshal([]byte(instancesJson.Content), &instances)
	if err != nil {
		return err
	}

	for timeOffset, instance := range instances {
		instanceID := instance.InstanceID

		if err := c.putDocumentInDatabase(instance, instanceID, "instances", timeOffset); err != nil {
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
			"last_updated": WellKnownTestTime.Add(time.Second * time.Duration(timeOffset)),
		},
	}

	_, err := c.MongoClient.Connection.Collection(collectionName).UpsertById(context.Background(), id, update)

	if err != nil {
		return err
	}
	return nil
}

func (c *DatasetComponent) aListOfNamedCantabularBlobsIsReturned() error {
	return c.APIFeature.IShouldReceiveTheFollowingJSONResponseWithStatus(
		"200",
		&godog.DocString{Content: `{ 
			"items": [
				{ "name": "blob 1" },
				{ "name": "blob 2" },
				{ "name": "blob 3" }
			]
		}`},
	)
}

func (c *DatasetComponent) iAccessTheCensusEndpoint() error {
	return c.APIFeature.IGet("/census")
}

func (c *DatasetComponent) iHaveSomeCantabularBlobs() error {
	c.fakeCantabularBlobs = []models.PopulationType{
		{Name: "blob 1"},
		{Name: "blob 2"},
		{Name: "blob 3"},
	}
	return nil
}
