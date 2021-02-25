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

// Mongo version here is overridden in the pipeline by the URL provided in the component.sh
const MongoVersion = "4.0.23"
const MongoPort = 27017
const DatabaseName = "testing"

var componentFlag = flag.Bool("component", false, "perform component tests")

type FeatureTest struct {
	MongoFeature *featuretest.MongoFeature
}

func (f *FeatureTest) InitializeScenario(ctx *godog.ScenarioContext) {
	authorizationFeature := featuretest.NewAuthorizationFeature()
	datasetFeature, err := steps_test.NewDatasetFeature(f.MongoFeature, authorizationFeature.FakeAuthService.ResolveURL(""))
	if err != nil {
		panic(err)
	}

	apiFeature := featuretest.NewAPIFeature(datasetFeature.InitialiseService)

	ctx.BeforeScenario(func(*godog.Scenario) {
		apiFeature.Reset()
		datasetFeature.Reset()
		f.MongoFeature.Reset()
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
		f.MongoFeature = featuretest.NewMongoFeature(featuretest.MongoOptions{Port: MongoPort, MongoVersion: MongoVersion, DatabaseName: DatabaseName})
	})
	ctx.AfterSuite(func() {
		f.MongoFeature.Close()
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
