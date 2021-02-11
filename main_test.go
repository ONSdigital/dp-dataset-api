package main

import (
	"flag"
	"os"
	"testing"

	steps_test "github.com/ONSdigital/dp-dataset-api/features/steps"
	featuretest "github.com/armakuni/dp-go-featuretest"
	"github.com/cucumber/godog"
	"github.com/cucumber/godog/colors"
)

var componentFlag = flag.Bool("component", false, "perform component tests")

type FeatureTest struct {
	Mongo *featuretest.MongoCapability
}

func (f *FeatureTest) InitializeScenario(ctx *godog.ScenarioContext) {
	authorizationFeature := featuretest.NewAuthorizationFeature()
	datasetFeature, err := steps_test.NewDatasetFeature(f.Mongo, authorizationFeature.FakeAuthService.ResolveURL(""))
	if err != nil {
		panic(err)
	}

	apiFeature := featuretest.NewAPIFeature(datasetFeature.InitialiseService)

	ctx.BeforeScenario(func(*godog.Scenario) {
		apiFeature.Reset()
		datasetFeature.Reset()
		f.Mongo.Reset()
		authorizationFeature.Reset()
	})

	ctx.AfterScenario(func(*godog.Scenario, error) {
		datasetFeature.Close()
		authorizationFeature.Close()
	})

	datasetFeature.RegisterSteps(ctx)
	apiFeature.RegisterSteps(ctx)
	authorizationFeature.RegisterSteps(ctx)
}

func (f *FeatureTest) InitializeTestSuite(ctx *godog.TestSuiteContext) {
	ctx.BeforeSuite(func() {
		f.Mongo = featuretest.NewMongoCapability(featuretest.MongoOptions{Port: 27017, MongoVersion: "4.0.5", DatabaseName: "testing"})
	})
	ctx.AfterSuite(func() {
		f.Mongo.Close()
	})
}

func TestMain(t *testing.T) {
	if *componentFlag {
		status := 0

		var opts = godog.Options{
			Output: colors.Colored(os.Stdout),
			Format: "pretty",
			Paths:  flag.Args(),
		}

		f := &FeatureTest{}

		status = godog.TestSuite{
			Name:                 "feature_tests",
			ScenarioInitializer:  f.InitializeScenario,
			TestSuiteInitializer: f.InitializeTestSuite,
			Options:              &opts,
		}.Run()

		os.Exit(status)
	} else {
		t.Skip("component flag required to run component tests")
	}
}
