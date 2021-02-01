package steps

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/cucumber/godog"
	"github.com/globalsign/mgo"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

func (f *APIFeature) IGet(path string) error {
	f.makeRequest("GET", path, nil)
	return nil
}

func (f *APIFeature) IPOSTTheFollowingTo(path string, body *godog.DocString) error {
	f.makeRequest("POST", path, []byte(body.Content))
	return nil
}

func (f *APIFeature) makeRequest(method, path string, data []byte) {
	f.startService()
	req := httptest.NewRequest(method, "http://"+f.httpServer.Addr+path, bytes.NewReader(data))
	req.Header.Set("Authorization", "ItDoesntMatter")

	w := httptest.NewRecorder()
	f.httpServer.Handler.ServeHTTP(w, req)

	f.httpResponse = w.Result()
}

func (f *APIFeature) IHaveTheseDatasets(datasetsJson *godog.DocString) error {

	datasets := []models.Dataset{}
	m := f.MongoClient

	err := json.Unmarshal([]byte(datasetsJson.Content), &datasets)
	if err != nil {
		return err
	}
	s := m.Session.Copy()
	defer s.Close()

	for _, datasetDoc := range datasets {
		f.putDatasetInDatabase(s, datasetDoc)
	}

	return nil
}

func (f *APIFeature) putDatasetInDatabase(s *mgo.Session, datasetDoc models.Dataset) {
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
		panic(err)
	}
}

func (f *APIFeature) IShouldReceiveTheFollowingResponse(expectedAPIResponse *godog.DocString) error {
	responseBody := f.httpResponse.Body
	body, _ := ioutil.ReadAll(responseBody)

	assert.Equal(f, strings.TrimSpace(expectedAPIResponse.Content), strings.TrimSpace(string(body)))

	return f.err
}

func (f *APIFeature) IShouldReceiveTheFollowingJSONResponse(expectedAPIResponse *godog.DocString) error {
	responseBody := f.httpResponse.Body
	body, _ := ioutil.ReadAll(responseBody)

	assert.JSONEq(f, expectedAPIResponse.Content, string(body))

	return f.err
}

func (f *APIFeature) TheHTTPStatusCodeShouldBe(expectedCodeStr string) error {
	expectedCode, err := strconv.Atoi(expectedCodeStr)
	if err != nil {
		return err
	}
	assert.Equal(f, expectedCode, f.httpResponse.StatusCode)
	return f.err
}

func (f *APIFeature) TheResponseHeaderShouldBe(headerName, expectedValue string) error {
	assert.Equal(f, expectedValue, f.httpResponse.Header.Get(headerName))
	return f.err
}

func (f *APIFeature) IShouldReceiveTheFollowingJSONResponseWithStatus(expectedCodeStr string, expectedBody *godog.DocString) error {
	if err := f.TheHTTPStatusCodeShouldBe(expectedCodeStr); err != nil {
		return err
	}
	if err := f.TheResponseHeaderShouldBe("Content-Type", "application/json"); err != nil {
		return err
	}
	return f.IShouldReceiveTheFollowingJSONResponse(expectedBody)
}

func (f *APIFeature) TheDocumentInTheDatabaseForIdShouldBe(documentId string, documentJson *godog.DocString) error {
	s := f.MongoClient.Session.Copy()
	defer s.Close()

	var expectedDataset models.Dataset

	err := json.Unmarshal([]byte(documentJson.Content), &expectedDataset)

	filterCursor := s.DB(f.MongoClient.Database).C("datasets").FindId(documentId)

	var document models.DatasetUpdate
	err = filterCursor.One(&document)
	if err != nil {
		return err
	}

	assert.Equal(f, documentId, document.ID)
	// FIXME: either test the intersection of the 2 JSONs, or use a table for the expected
	assert.Equal(f, expectedDataset.Title, document.Next.Title)
	assert.Equal(f, "created", document.Next.State)

	return f.err
}

func (f *APIFeature) IAmNotIdentified() error {
	f.FakeAuthService.NewHandler().Get("/identity").Reply(401)
	return nil
}

func (f *APIFeature) IAmIdentifiedAs(username string) error {
	f.FakeAuthService.NewHandler().Get("/identity").Reply(200).BodyString(`{ "identifier": "` + username + `"}`)
	return nil
}

func (f *APIFeature) PrivateEndpointsAreEnabled() error {
	f.Config.EnablePrivateEndpoints = true
	return nil
}

func (f *APIFeature) startService() {
	if err := f.svc.Run(context.Background(), "1", "", "", f.errorChan); err != nil {
		panic(err)
	}
}
