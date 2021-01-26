package main_test

import (
	"flag"
	"os"
	"testing"

	"github.com/ONSdigital/dp-dataset-api/features/steps"
	"github.com/cucumber/godog"
	"github.com/cucumber/godog/colors"
)

func TestMain(m *testing.M) {
	var opts = godog.Options{
		Output: colors.Colored(os.Stdout),
		Format: "pretty", // can define default values
	}
	godog.BindFlags("godog.", flag.CommandLine, &opts) // godog v0.10.0 and earlier
	godog.BindCommandLineFlags("godog.", &opts)        // godog v0.11.0 (latest)
	flag.Parse()

	opts.Paths = flag.Args()

	status := godog.TestSuite{
		Name:                 "dp-dataset-api",
		TestSuiteInitializer: steps.InitializeTestSuite,
		ScenarioInitializer:  steps.InitializeScenario,
		Options:              &opts,
	}.Run()

	// Optional: Run `testing` package's logic besides godogt.
	if st := m.Run(); st > status {
		status = st
	}

	os.Exit(status)
}
