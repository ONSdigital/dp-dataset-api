package steps

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"time"

	"github.com/ONSdigital/dp-dataset-api/models"
	"github.com/cucumber/godog"
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
	req := httptest.NewRequest("GET", "http://"+f.httpServer.Addr+path, nil)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.httpServer.Handler.ServeHTTP(w, req)

	f.httpResponse = w.Result()

	return nil
}

func (f *APIFeature) IHaveTheseDatasets(datasetsJson *godog.DocString) error {

	datasets := []models.Dataset{}
	m := f.MongoClient

	err := json.Unmarshal([]byte(datasetsJson.Content), &datasets)
	if err != nil {
		return err
	}

	datasetDoc := datasets[0]
	fmt.Println(datasetDoc)

	fmt.Println("========================")
	fmt.Println(m.Database)

	s := m.Session.Copy()
	defer s.Close()

	datasetID := datasetDoc.ID

	datasetUp := models.DatasetUpdate{
		ID:   datasetID,
		Next: &datasetDoc,
	}

	update := bson.M{
		"$set": datasetUp,
		"$setOnInsert": bson.M{
			"last_updated": time.Now(),
		},
	}

	_, err = s.DB(m.Database).C("datasets").UpsertId(datasetID, update)
	if err != nil {
		panic(err)
	}

	return nil
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
