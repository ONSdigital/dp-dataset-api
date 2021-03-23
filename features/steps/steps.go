package steps

import (
	"encoding/json"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/cucumber/godog"
	"github.com/globalsign/mgo/bson"
	"github.com/stretchr/testify/assert"
)

func (f *DatasetComponent) RegisterSteps(ctx *godog.ScenarioContext) {
	ctx.Step(`^private endpoints are enabled$`, f.privateEndpointsAreEnabled)
	ctx.Step(`^the document in the database for id "([^"]*)" should be:$`, f.theDocumentInTheDatabaseForIdShouldBe)
	ctx.Step(`^there are no datasets$`, f.thereAreNoDatasets)
	ctx.Step(`^I have these datasets:$`, f.iHaveTheseDatasets)
	ctx.Step(`^I have these editions:$`, f.iHaveTheseEditions)
	ctx.Step(`^I have these versions:$`, f.iHaveTheseVersions)
	ctx.Step(`^I have these dimensions:$`, f.iHaveTheseDimensions)
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

	json.Unmarshal([]byte(documentJson.Content), &expectedDataset)

	filterCursor := s.DB(f.MongoClient.Database).C("datasets").FindId(documentId)

	var link models.DatasetUpdate

	if err := filterCursor.One(&link); err != nil {
		return err
	}

	assert.Equal(&f.ErrorFeature, documentId, link.ID)

	document := link.Next

	assert.Equal(&f.ErrorFeature, expectedDataset.Title, document.Title)
	assert.Equal(&f.ErrorFeature, "created", document.State)

	return f.ErrorFeature.StepError()
}

func (f *DatasetComponent) iHaveTheseEditions(editionsJson *godog.DocString) error {

	editions := []models.Edition{}

	err := json.Unmarshal([]byte(editionsJson.Content), &editions)
	if err != nil {
		return err
	}

	for time, editionDoc := range editions {
		editionID := editionDoc.ID

		editionUp := models.EditionUpdate{
			ID:      editionID,
			Next:    &editionDoc,
			Current: &editionDoc,
		}

		err = f.putDocumentInDatabase(editionUp, editionID, "editions", time)
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

	for time, datasetDoc := range datasets {
		datasetID := datasetDoc.ID

		datasetUp := models.DatasetUpdate{
			ID:      datasetID,
			Next:    &datasetDoc,
			Current: &datasetDoc,
		}
		if err := f.putDocumentInDatabase(datasetUp, datasetID, "datasets", time); err != nil {
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

	for time, version := range versions {
		versionID := version.ID
		version.Links.Version = &models.LinkObject{
			HRef: version.Links.Self.HRef,
		}

		f.putDocumentInDatabase(version, versionID, "instances", time)
	}

	return nil
}

func (f *DatasetComponent) iHaveTheseDimensions(dimensionsJson *godog.DocString) error {
	dimensions := []models.DimensionOption{}

	err := json.Unmarshal([]byte(dimensionsJson.Content), &dimensions)
	if err != nil {
		return err
	}

	for time, dimension := range dimensions {
		dimensionID := ""

		f.putDocumentInDatabase(dimension, dimensionID, "dimension.options", time)
	}

	return nil
}

func (f *DatasetComponent) putDocumentInDatabase(document interface{}, id, collectionName string, time int) error {
	s := f.MongoClient.Session.Copy()
	defer s.Close()

	update := bson.M{
		"$set": document,
		"$setOnInsert": bson.M{
			"last_updated": time,
		},
	}
	_, err := s.DB(f.MongoClient.Database).C(collectionName).UpsertId(id, update)
	if err != nil {
		return err
	}
	return nil
}
