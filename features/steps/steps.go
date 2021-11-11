package steps

import (
	"encoding/json"
	"time"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/cucumber/godog"
	"github.com/globalsign/mgo/bson"
	"github.com/stretchr/testify/assert"
)

var WellKnownTestTime time.Time

func init() {
	WellKnownTestTime, _ = time.Parse("2006-01-02T15:04:05Z", "2021-01-01T00:00:00Z")
}

func (f *DatasetComponent) RegisterSteps(ctx *godog.ScenarioContext) {
	ctx.Step(`^private endpoints are enabled$`, f.privateEndpointsAreEnabled)
	ctx.Step(`^the document in the database for id "([^"]*)" should be:$`, f.theDocumentInTheDatabaseForIdShouldBe)
	ctx.Step(`^there are no datasets$`, f.thereAreNoDatasets)
	ctx.Step(`^I have these datasets:$`, f.iHaveTheseDatasets)
	ctx.Step(`^I have these editions:$`, f.iHaveTheseEditions)
	ctx.Step(`^I have these versions:$`, f.iHaveTheseVersions)
	ctx.Step(`^I have these dimensions:$`, f.iHaveTheseDimensions)
	ctx.Step(`^I have these instances:$`, f.iHaveTheseInstances)
}

func (f *DatasetComponent) thereAreNoDatasets() error {
	return f.MongoClient.Session.Copy().DB(f.MongoClient.Database).DropDatabase()
}

func (f *DatasetComponent) privateEndpointsAreEnabled() error {
	f.Config.EnablePrivateEndpoints = true
	return nil
}

func (f *DatasetComponent) theDocumentInTheDatabaseForIdShouldBe(documentId string, documentJson *godog.DocString) error {
	s := f.MongoClient.Session.Copy()
	defer s.Close()

	var expectedDataset models.Dataset

	if err := json.Unmarshal([]byte(documentJson.Content), &expectedDataset); err != nil {
		return err
	}

	filterCursor := s.DB(f.MongoClient.Database).C("datasets").FindId(documentId)

	var link models.DatasetUpdate

	if err := filterCursor.One(&link); err != nil {
		return err
	}

	assert.Equal(&f.ErrorFeature, documentId, link.ID)

	document := link.Next

	assert.Equal(&f.ErrorFeature, expectedDataset.Title, document.Title)
	assert.Equal(&f.ErrorFeature, expectedDataset.State, document.State)

	return f.ErrorFeature.StepError()
}

func (f *DatasetComponent) iHaveTheseEditions(editionsJson *godog.DocString) error {

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

		err = f.putDocumentInDatabase(editionUp, editionID, "editions", timeOffset)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *DatasetComponent) iHaveTheseDatasets(datasetsJson *godog.DocString) error {

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
		if err := f.putDocumentInDatabase(datasetUp, datasetID, "datasets", timeOffset); err != nil {
			return err
		}
	}

	return nil
}

func (f *DatasetComponent) iHaveTheseVersions(versionsJson *godog.DocString) error {
	versions := []models.Version{}

	err := json.Unmarshal([]byte(versionsJson.Content), &versions)
	if err != nil {
		return err
	}

	for timeOffset, version := range versions {
		versionID := version.ID
		version.Links.Version = &models.LinkObject{
			HRef: version.Links.Self.HRef,
		}
		if err := f.putDocumentInDatabase(version, versionID, "instances", timeOffset); err != nil {
			return err
		}
	}

	return nil
}

func (f *DatasetComponent) iHaveTheseDimensions(dimensionsJson *godog.DocString) error {
	dimensions := []models.DimensionOption{}

	err := json.Unmarshal([]byte(dimensionsJson.Content), &dimensions)
	if err != nil {
		return err
	}

	for timeOffset, dimension := range dimensions {
		dimensionID := dimension.Option
		if err := f.putDocumentInDatabase(dimension, dimensionID, "dimension.options", timeOffset); err != nil {
			return err
		}
	}

	return nil
}

func (f *DatasetComponent) iHaveTheseInstances(instancesJson *godog.DocString) error {
	instances := []models.Instance{}

	err := json.Unmarshal([]byte(instancesJson.Content), &instances)
	if err != nil {
		return err
	}

	for timeOffset, instance := range instances {
		instanceID := instance.InstanceID
		if err := f.putDocumentInDatabase(instance, instanceID, "instances", timeOffset); err != nil {
			return err
		}
	}

	return nil
}

func (f *DatasetComponent) putDocumentInDatabase(document interface{}, id, collectionName string, timeOffset int) error {
	s := f.MongoClient.Session.Copy()
	defer s.Close()

	update := bson.M{
		"$set": document,
		"$setOnInsert": bson.M{
			"last_updated": WellKnownTestTime.Add(time.Second * time.Duration(timeOffset)),
		},
	}
	_, err := s.DB(f.MongoClient.Database).C(collectionName).UpsertId(id, update)
	if err != nil {
		return err
	}
	return nil
}
