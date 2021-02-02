package steps_test

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"

	"github.com/cucumber/godog"
	"github.com/stretchr/testify/assert"
)

type APIFeature struct {
	ErrorFeature
	httpServer        *http.Server
	httpResponse      *http.Response
	BeforeRequestHook func() error
	requestHeaders    map[string]string
}

func NewAPIFeature(httpServer *http.Server) *APIFeature {
	return &APIFeature{
		httpServer:     httpServer,
		requestHeaders: make(map[string]string),
	}
}

func (f *APIFeature) Reset() {
	f.requestHeaders = make(map[string]string)
}

func (f *APIFeature) RegisterSteps(ctx *godog.ScenarioContext) {
	ctx.Step(`^I set the "([^"]*)" header to "([^"]*)"$`, f.ISetTheHeaderTo)
	ctx.Step(`^I am authorised$`, f.IAmAuthorised)
	ctx.Step(`^I am not authorised$`, f.IAmNotAuthorised)
	ctx.Step(`^I GET "([^"]*)"$`, f.IGet)
	ctx.Step(`^I POST the following to "([^"]*)":$`, f.IPOSTTheFollowingTo)
	ctx.Step(`^the HTTP status code should be "([^"]*)"$`, f.TheHTTPStatusCodeShouldBe)
	ctx.Step(`^the response header "([^"]*)" should be "([^"]*)"$`, f.TheResponseHeaderShouldBe)
	ctx.Step(`^I should receive the following response:$`, f.IShouldReceiveTheFollowingResponse)
	ctx.Step(`^I should receive the following JSON response:$`, f.IShouldReceiveTheFollowingJSONResponse)
	ctx.Step(`^I should receive the following JSON response with status "([^"]*)":$`, f.IShouldReceiveTheFollowingJSONResponseWithStatus)
}

func (f *APIFeature) ISetTheHeaderTo(header, value string) error {
	f.requestHeaders[header] = value
	return nil
}

func (f *APIFeature) IAmAuthorised() error {
	f.ISetTheHeaderTo("Authorization", "bearer SomeFakeToken")
	return nil
}

func (f *APIFeature) IAmNotAuthorised() error {
	delete(f.requestHeaders, "Authorization")
	return nil
}

func (f *APIFeature) IGet(path string) error {
	return f.makeRequest("GET", path, nil)
}

func (f *APIFeature) IPOSTTheFollowingTo(path string, body *godog.DocString) error {
	return f.makeRequest("POST", path, []byte(body.Content))
}

func (f *APIFeature) makeRequest(method, path string, data []byte) error {
	if f.BeforeRequestHook != nil {
		if err := f.BeforeRequestHook(); err != nil {
			return err
		}
	}
	req := httptest.NewRequest(method, "http://"+f.httpServer.Addr+path, bytes.NewReader(data))
	req.Header.Set("Authorization", "ItDoesntMatter")

	w := httptest.NewRecorder()
	f.httpServer.Handler.ServeHTTP(w, req)

	f.httpResponse = w.Result()
	return nil
}

func (f *APIFeature) IShouldReceiveTheFollowingResponse(expectedAPIResponse *godog.DocString) error {
	responseBody := f.httpResponse.Body
	body, _ := ioutil.ReadAll(responseBody)

	assert.Equal(f, strings.TrimSpace(expectedAPIResponse.Content), strings.TrimSpace(string(body)))

	return f.StepError()
}

func (f *APIFeature) IShouldReceiveTheFollowingJSONResponse(expectedAPIResponse *godog.DocString) error {
	responseBody := f.httpResponse.Body
	body, _ := ioutil.ReadAll(responseBody)

	assert.JSONEq(f, expectedAPIResponse.Content, string(body))

	return f.StepError()
}

func (f *APIFeature) TheHTTPStatusCodeShouldBe(expectedCodeStr string) error {
	expectedCode, err := strconv.Atoi(expectedCodeStr)
	if err != nil {
		return err
	}
	assert.Equal(f, expectedCode, f.httpResponse.StatusCode)
	return f.StepError()
}

func (f *APIFeature) TheResponseHeaderShouldBe(headerName, expectedValue string) error {
	assert.Equal(f, expectedValue, f.httpResponse.Header.Get(headerName))
	return f.StepError()
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
