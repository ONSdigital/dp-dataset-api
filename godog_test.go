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
	flag.Parse()
	var opts = godog.Options{
		Output: colors.Colored(os.Stdout),
		Format: "progress", // can define default values
	}
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
