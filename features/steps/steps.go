package steps

import (
	"encoding/json"
	"time"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/cucumber/godog"
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/stretchr/testify/assert"
)

func (f *DatasetComponent) RegisterSteps(ctx *godog.ScenarioContext) {
	ctx.Step(`^private endpoints are enabled$`, f.privateEndpointsAreEnabled)
	ctx.Step(`^I have these datasets:$`, f.iHaveTheseDatasets)
	ctx.Step(`^the document in the database for id "([^"]*)" should be:$`, f.theDocumentInTheDatabaseForIdShouldBe)
	ctx.Step(`^there are no datasets$`, f.thereAreNoDatasets)
	ctx.Step(`^I have these editions:$`, f.iHaveTheseEditions)
	ctx.Step(`^I have these versions:$`, f.iHaveTheseVersions)
}

func (f *DatasetComponent) iHaveTheseDatasets(datasetsJson *godog.DocString) error {

	datasets := []models.Dataset{}
	m := f.MongoClient

	err := json.Unmarshal([]byte(datasetsJson.Content), &datasets)
	if err != nil {
		return err
	}
	s := m.Session.Copy()
	defer s.Close()

	for _, datasetDoc := range datasets {
		if err := f.putDatasetInDatabase(s, datasetDoc); err != nil {
			return err
		}
	}

	return nil
}

func (f *DatasetComponent) thereAreNoDatasets() error {
	return f.MongoClient.Session.Copy().DB(f.MongoClient.Database).DropDatabase()
}

func (f *DatasetComponent) putDatasetInDatabase(s *mgo.Session, datasetDoc models.Dataset) error {
	datasetID := datasetDoc.ID

	datasetUp := models.DatasetUpdate{
		ID:      datasetID,
		Next:    &datasetDoc,
		Current: &datasetDoc,
	}

	update := bson.M{
		"$set": datasetUp,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}
	_, err := s.DB(f.MongoClient.Database).C("datasets").UpsertId(datasetID, update)
	if err != nil {
		return err
	}
	return nil
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
	m := f.MongoClient

	err := json.Unmarshal([]byte(editionsJson.Content), &editions)
	if err != nil {
		return err
	}
	s := m.Session.Copy()
	defer s.Close()

	for _, editionDoc := range editions {
		editionID := editionDoc.ID

		editionUp := models.EditionUpdate{
			ID:      editionID,
			Next:    &editionDoc,
			Current: &editionDoc,
		}

		update := bson.M{
			"$set": editionUp,
			"$setOnInsert": bson.M{
				"last_updated": time.Now(),
			},
		}
		_, err := s.DB(f.MongoClient.Database).C("editions").UpsertId(editionID, update)
		if err != nil {
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

	for _, version := range versions {
		versionID := version.ID

		f.putDocumentInDatabase(version, versionID, "instances")
	}

	return nil
}

func (f *DatasetComponent) putDocumentInDatabase(document interface{}, id, collectionName string) error {

	s := f.MongoClient.Session.Copy()
	defer s.Close()

	update := bson.M{
		"$set": document,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}
	_, err := s.DB(f.MongoClient.Database).C(collectionName).UpsertId(id, update)
	if err != nil {
		return err
	}
	return nil
}
