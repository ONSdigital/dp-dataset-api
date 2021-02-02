package main_test

import (
	"io/ioutil"
	"log"

	steps_test "github.com/ONSdigital/dp-dataset-api/features/steps"
	"github.com/cucumber/godog"
)

func InitializeScenario(ctx *godog.ScenarioContext) {
	mongoCapability := steps_test.NewMongoCapability(steps_test.MongoOptions{27017, "4.0.5", log.New(ioutil.Discard, "", 0)})
	datasetFeature := steps_test.NewDatasetFeature(mongoCapability)
	apiFeature := steps_test.NewAPIFeature(datasetFeature.HTTPServer)
	apiFeature.BeforeRequestHook = datasetFeature.BeforeRequestHook

	ctx.BeforeScenario(func(*godog.Scenario) {
		apiFeature.Reset()
		datasetFeature.Reset()
		mongoCapability.Reset()
	})

	ctx.AfterScenario(func(*godog.Scenario, error) {
		datasetFeature.Close()
		mongoCapability.Close()

	})

	datasetFeature.RegisterSteps(ctx)
	apiFeature.RegisterSteps(ctx)
}

func InitializeTestSuite(ctx *godog.TestSuiteContext) {
	ctx.BeforeSuite(func() {
	})
}
