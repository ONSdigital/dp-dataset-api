package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	componenttest "github.com/ONSdigital/dp-component-test"
	"github.com/ONSdigital/dp-dataset-api/features/steps"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/cucumber/godog"
	"github.com/cucumber/godog/colors"
)

const mongoVersion = "4.4.8"
const databaseName = "testing"
const replicaSetName = "rs0"

var componentFlag = flag.Bool("component", false, "perform component tests")

type ComponentTest struct {
	MongoFeature *componenttest.MongoFeature
}

func (f *ComponentTest) InitializeScenario(godogCtx *godog.ScenarioContext) {
	authorizationFeature := componenttest.NewAuthorizationFeature()
	datasetFeature, err := steps.NewDatasetComponent(f.MongoFeature.Server.URI(), authorizationFeature.FakeAuthService.ResolveURL(""))
	if err != nil {
		panic(err)
	}

	apiFeature := componenttest.NewAPIFeature(datasetFeature.InitialiseService)

	godogCtx.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		apiFeature.Reset()
		if err := datasetFeature.Reset(); err != nil {
			panic(err)
		}
		if err := f.MongoFeature.Reset(); err != nil {
			log.Error(context.Background(), "failed to reset mongo feature", err)
		}
		authorizationFeature.Reset()
		return ctx, nil
	})

	godogCtx.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		datasetFeature.Close()
		authorizationFeature.Close()
		return ctx, nil
	})

	datasetFeature.RegisterSteps(godogCtx)
	apiFeature.RegisterSteps(godogCtx)
	authorizationFeature.RegisterSteps(godogCtx)
}

func (f *ComponentTest) InitializeTestSuite(ctx *godog.TestSuiteContext) {
	ctx.BeforeSuite(func() {
		f.MongoFeature = componenttest.NewMongoFeature(componenttest.MongoOptions{MongoVersion: mongoVersion, DatabaseName: databaseName, ReplicaSetName: replicaSetName})
	})
	ctx.AfterSuite(func() {
		f.MongoFeature.Close()
	})
}

func TestComponent(t *testing.T) {
	if *componentFlag {
		var opts = godog.Options{
			Output: colors.Colored(os.Stdout),
			Format: "pretty",
			Paths:  flag.Args(),
		}

		f := &ComponentTest{}

		status := godog.TestSuite{
			Name:                 "feature_tests",
			ScenarioInitializer:  f.InitializeScenario,
			TestSuiteInitializer: f.InitializeTestSuite,
			Options:              &opts,
		}.Run()

		fmt.Println("=================================")
		fmt.Printf("Component test coverage: %.2f%%\n", testing.Coverage()*100)
		fmt.Println("=================================")

		if status > 0 {
			t.Errorf("component testing from godog test suite failed with status %d", status)
		}
	} else {
		t.Skip("component flag required to run component tests")
	}
}
