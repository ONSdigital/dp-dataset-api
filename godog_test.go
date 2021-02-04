package main_test

import (
	"io/ioutil"
	"log"

	steps_test "github.com/ONSdigital/dp-dataset-api/features/steps"
	featuretest "github.com/armakuni/dp-go-featuretest"
	"github.com/cucumber/godog"
)

func InitializeScenario(ctx *godog.ScenarioContext) {
	mongoCapability := featuretest.NewMongoCapability(featuretest.MongoOptions{27017, "4.0.5", log.New(ioutil.Discard, "", 0)})
	datasetFeature := steps_test.NewDatasetFeature(mongoCapability)

	apiFeature := featuretest.NewAPIFeature(datasetFeature.InitialiseService)

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
