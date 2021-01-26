package steps

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"

	"github.com/cucumber/godog"
	"github.com/stretchr/testify/assert"
)

type TestContext struct {
	response *http.Response
}

func (f *apiFeature) Errorf(format string, args ...interface{}) {
	f.err = fmt.Errorf(format, args...)
}

func (f *apiFeature) iGET(path string) error {
	req := httptest.NewRequest("GET", "http://"+f.httpServer.Addr+path, nil)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	f.httpServer.Handler.ServeHTTP(w, req)

	f.httpResponse = w.Result()

	return nil
}

func (f *apiFeature) iHaveTheseDatasets(datasets *godog.DocString) error {

	err := json.Unmarshal([]byte(datasets.Content), &f.Datasets)
	if err != nil {
		return err
	}

	return nil
}

func (f *apiFeature) iShouldReceiveTheFollowingJSONResponse(expectedAPIResponse *godog.DocString) error {
	responseBody := f.httpResponse.Body
	body, _ := ioutil.ReadAll(responseBody)

	assert.JSONEq(f, string(body), expectedAPIResponse.Content)

	return f.err
}

func (f *apiFeature) theHTTPStatusCodeShouldBe(expectedCodeStr string) error {
	expectedCode, err := strconv.Atoi(expectedCodeStr)
	if err != nil {
		return err
	}
	assert.Equal(f, expectedCode, f.httpResponse.StatusCode)
	return f.err
}

func (f *apiFeature) theResponseHeaderShouldBe(headerName, expectedValue string) error {
	assert.Equal(f, expectedValue, f.httpResponse.Header.Get(headerName))
	return f.err
}

func (f *apiFeature) iShouldReceiveTheFollowingJSONResponseWithStatus(expectedCodeStr string, expectedBody *godog.DocString) error {
	if err := f.theHTTPStatusCodeShouldBe(expectedCodeStr); err != nil {
		return err
	}
	if err := f.theResponseHeaderShouldBe("Content-Type", "application/json"); err != nil {
		return err
	}
	return f.iShouldReceiveTheFollowingJSONResponse(expectedBody)
}

func InitializeScenario(ctx *godog.ScenarioContext) {
	var f *apiFeature = newAPIFeature()

	ctx.BeforeScenario(func(*godog.Scenario) {
		f.Reset()
	})

	ctx.AfterScenario(func(*godog.Scenario, error) {
		f.Close()
	})

	ctx.Step(`^I GET "([^"]*)"$`, f.iGET)
	ctx.Step(`^I should receive the following JSON response:$`, f.iShouldReceiveTheFollowingJSONResponse)
	ctx.Step(`^the HTTP status code should be "([^"]*)"$`, f.theHTTPStatusCodeShouldBe)
	ctx.Step(`^the response header "([^"]*)" should be "([^"]*)"$`, f.theResponseHeaderShouldBe)
	ctx.Step(`^I should receive the following JSON response with status "([^"]*)":$`, f.iShouldReceiveTheFollowingJSONResponseWithStatus)
	ctx.Step(`^I have these datasets:$`, f.iHaveTheseDatasets)
}

func InitializeTestSuite(ctx *godog.TestSuiteContext) {
	ctx.BeforeSuite(func() {
	})
}
