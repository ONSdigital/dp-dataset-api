package steps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/cucumber/godog"
	"github.com/globalsign/mgo"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
)

type TestContext struct {
	response *http.Response
}

func (f *APIFeature) Errorf(format string, args ...interface{}) {
	f.err = fmt.Errorf(format, args...)
}

func (f *APIFeature) IGet(path string) error {
	f.startService()

	f.get(path)

	return nil
}

func (f *APIFeature) IPOSTTheFollowingTo(path string, body *godog.DocString) error {
	f.startService()

	dataset := models.Dataset{}

	err := json.Unmarshal([]byte(body.Content), &dataset)

	payload := map[string]string{
		"title": dataset.Title,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	f.post(path, data)

	return nil
}

func (f *APIFeature) post(path string, data []byte) {
	f.makeRequest("POST", path, data)
}

func (f *APIFeature) get(path string) {
	f.makeRequest("GET", path, nil)
}

func (f *APIFeature) makeRequest(method, path string, data []byte) {
	req := httptest.NewRequest(method, "http://"+f.httpServer.Addr+path, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
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

func (f *APIFeature) IAmNotIdentified() error {
	return nil
}

func (f *APIFeature) PrivateEndpointsAreEnabled() error {

	fmt.Printf("config: %p\n", f.Config)
	fmt.Printf("EnablePrivateEndpoints in steps: %p\n", &f.Config.EnablePrivateEndpoints)

	f.Config.EnablePrivateEndpoints = true
	return nil
}

func (f *APIFeature) startService() {
	if err := f.svc.Run(context.Background(), "1", "", "", f.errorChan); err != nil {
		panic(err)
	}
}
