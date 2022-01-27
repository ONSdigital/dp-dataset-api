package cantabular_test

import (
	"context"
	"errors"
	"github.com/ONSdigital/dp-dataset-api/cantabular/mocks"
	"github.com/ONSdigital/dp-dataset-api/config"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	"strings"
	"testing"
)

import (
	"github.com/ONSdigital/dp-api-clients-go/v2/cantabular"
	apiCantabular "github.com/ONSdigital/dp-dataset-api/cantabular"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
)

func TestAdapterConstruction(t *testing.T) {

	var actualCantabularConfig cantabular.Config
	var actualUserAgent dphttp.Clienter

	buildTestClient := func(cantabularConfig cantabular.Config, userAgent dphttp.Clienter) apiCantabular.Client {
		actualCantabularConfig = cantabularConfig
		actualUserAgent = userAgent
		return nil //&mocks.ClientMock{}
	}

	expectedCantabularConfig := config.CantabularConfig{
		CantabularURL:         "cantabular url",
		CantabularExtURL:      "cantabular ext url",
		DefaultRequestTimeout: 1234,
	}

	_ = apiCantabular.NewCantabularAdapterForStrategy(expectedCantabularConfig, buildTestClient)
	if actualUserAgent == nil {
		t.Error("Incorrect user agent supplied")
	}
	if actualCantabularConfig.Host != expectedCantabularConfig.CantabularURL {
		t.Error("Mismatched cantabular host configuration")
	}
	if actualCantabularConfig.ExtApiHost != expectedCantabularConfig.CantabularExtURL {
		t.Error("Mismatched cantabular external API host configuration")
	}
	if actualCantabularConfig.GraphQLTimeout != expectedCantabularConfig.DefaultRequestTimeout {
		t.Error("Mismatched cantabular graphQL timeout configuration")
	}
}

func TestAdapterChecker(t *testing.T) {

	clientMock := mocks.ClientMock{}

	buildTestClient := func(cantabularConfig cantabular.Config, userAgent dphttp.Clienter) apiCantabular.Client {
		return &clientMock
	}

	clientMock.CheckerFunc = func(ctx context.Context, state *healthcheck.CheckState) error {
		return nil
	}

	clientMock.CheckerAPIExtFunc = func(ctx context.Context, state *healthcheck.CheckState) error {
		return nil
	}

	adapter := apiCantabular.NewCantabularAdapterForStrategy(config.CantabularConfig{}, buildTestClient)

	expectedContext := context.Background()
	expectedCheckState := healthcheck.CheckState{}
	actualError := adapter.Checker(expectedContext, &expectedCheckState)

	if actualError != nil {
		t.Errorf("Unexpected error returned: %v", actualError)
	}
	actualCheckerCalls := clientMock.CheckerCalls()
	if len(actualCheckerCalls) != 1 {
		t.Errorf("Expected 1 call to client Checker but found %v", len(actualCheckerCalls))
	}
	actualCheckerContextArg := actualCheckerCalls[0].Ctx
	if actualCheckerContextArg != expectedContext {
		t.Errorf("Expected checker argument ctx to equal %v but found %v", expectedContext, actualCheckerContextArg)
	}
	actualCheckerCheckStateArg := actualCheckerCalls[0].State
	if actualCheckerCheckStateArg != &expectedCheckState {
		t.Errorf("Expected checker argument state pointer to equal %p but found %p", &expectedCheckState, actualCheckerCheckStateArg)
	}

	actualCheckerAPIExtCalls := clientMock.CheckerAPIExtCalls()
	if len(actualCheckerAPIExtCalls) != 1 {
		t.Errorf("Expected 1 call to client CheckerAPIExt but found %v", len(actualCheckerAPIExtCalls))
	}
	actualCheckerAPIExtContextArg := actualCheckerAPIExtCalls[0].Ctx
	if actualCheckerAPIExtContextArg != expectedContext {
		t.Errorf("Expected CheckerAPIExt argument ctx to equal %v but found %v", expectedContext, actualCheckerContextArg)
	}
	actualCheckerAPIExtStateArg := actualCheckerAPIExtCalls[0].State
	if actualCheckerAPIExtStateArg != &expectedCheckState {
		t.Errorf("Expected CheckerAPIExt state pointer to equal %p but found %p", &expectedCheckState, actualCheckerCheckStateArg)
	}

}

func TestAdapterCheckerUnhappyClientChecker(t *testing.T) {

	clientMock := mocks.ClientMock{}

	buildTestClient := func(cantabularConfig cantabular.Config, userAgent dphttp.Clienter) apiCantabular.Client {
		return &clientMock
	}

	expectedError := errors.New("expected error")

	clientMock.CheckerFunc = func(ctx context.Context, state *healthcheck.CheckState) error {
		return expectedError
	}

	adapter := apiCantabular.NewCantabularAdapterForStrategy(config.CantabularConfig{}, buildTestClient)

	actualError := adapter.Checker(context.Background(), &healthcheck.CheckState{})

	if actualError != expectedError {
		t.Errorf("Expected error: %+v, but got: %+v", expectedError, actualError)
	}
}

func TestAdapterCheckerUnhappyClientCheckerAPIExt(t *testing.T) {
	clientMock := mocks.ClientMock{}

	buildTestClient := func(cantabularConfig cantabular.Config, userAgent dphttp.Clienter) apiCantabular.Client {
		return &clientMock
	}

	expectedError := errors.New("expected error")

	clientMock.CheckerFunc = func(ctx context.Context, state *healthcheck.CheckState) error {
		return nil
	}
	clientMock.CheckerAPIExtFunc = func(ctx context.Context, state *healthcheck.CheckState) error {
		return expectedError
	}

	adapter := apiCantabular.NewCantabularAdapterForStrategy(config.CantabularConfig{}, buildTestClient)

	actualError := adapter.Checker(context.Background(), &healthcheck.CheckState{})

	if actualError != expectedError {
		t.Errorf("Expected error: %+v, but got: %+v", expectedError, actualError)
	}
}

func TestAdapterPopulationTypesHappy(t *testing.T) {
	clientMock := mocks.ClientMock{}

	buildTestClient := func(cantabularConfig cantabular.Config, userAgent dphttp.Clienter) apiCantabular.Client {
		return &clientMock
	}
	expected := []string{"hello", "world"}
	clientMock.GetPopulationTypesFunc = func(ctx context.Context) ([]string, error) {
		return expected, nil
	}
	adapter := apiCantabular.NewCantabularAdapterForStrategy(config.CantabularConfig{}, buildTestClient)

	expectedContext := context.Background()
	actualPopulationTypes, actualError := adapter.PopulationTypes(expectedContext)
	if actualError != nil {
		t.Errorf("Unexpected error: %v", actualError)
	}
	if len(actualPopulationTypes) != 2 {
		t.Errorf("Expected 2 population types to be returned but found %v", len(actualPopulationTypes))
	}
	actualPopulationTypeNames := strings.Join(
		[]string{actualPopulationTypes[0].Name, actualPopulationTypes[1].Name},
		",")
	if actualPopulationTypeNames != "hello,world" {
		t.Errorf("Expected population types: hello,world, but got: %v", actualPopulationTypeNames)
	}
}

func TestAdapterPopulationTypesUnhappy(t *testing.T) {
	clientMock := mocks.ClientMock{}

	buildTestClient := func(cantabularConfig cantabular.Config, userAgent dphttp.Clienter) apiCantabular.Client {
		return &clientMock
	}
	expectedError := errors.New("Oh no")
	clientMock.GetPopulationTypesFunc = func(ctx context.Context) ([]string, error) {
		return nil, expectedError
	}
	adapter := apiCantabular.NewCantabularAdapterForStrategy(config.CantabularConfig{}, buildTestClient)

	_, actualError := adapter.PopulationTypes(context.Background())
	if actualError != expectedError {
		t.Errorf("Expected error: %v, but got: %v", expectedError, actualError)
	}
}
