package main_test

import (
	"github.com/ONSdigital/dp-dataset-api/features/steps"
	"github.com/cucumber/godog"
)

func InitializeScenario(ctx *godog.ScenarioContext) {
	var f *steps.APIFeature = steps.NewAPIFeature()

	ctx.BeforeScenario(func(*godog.Scenario) {
		f.Reset()
	})

	ctx.AfterScenario(func(*godog.Scenario, error) {
		f.Close()
	})

	ctx.Step(`^I have these datasets:$`, f.IHaveTheseDatasets)
	ctx.Step(`^I GET "([^"]*)"$`, f.IGet)
	ctx.Step(`^the HTTP status code should be "([^"]*)"$`, f.TheHTTPStatusCodeShouldBe)
	ctx.Step(`^the response header "([^"]*)" should be "([^"]*)"$`, f.TheResponseHeaderShouldBe)
	ctx.Step(`^I should receive the following JSON response:$`, f.IShouldReceiveTheFollowingJSONResponse)
	ctx.Step(`^I should receive the following JSON response with status "([^"]*)":$`, f.IShouldReceiveTheFollowingJSONResponseWithStatus)
}

func InitializeTestSuite(ctx *godog.TestSuiteContext) {
	ctx.BeforeSuite(func() {
	})
}
